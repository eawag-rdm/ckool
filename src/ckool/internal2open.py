# _*_ coding: utf-8 _*_

import hashlib
import os
import re
import time
import xml.etree.ElementTree as ET

import ckanapi
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from utilities import get_secret

# "Names" of secrets are either environment variables or the path in
#  the password manager "pass"
APIKEY_ERIC_NAME = "rdm_apikey@data.eawag.ch"
APIKEY_ERIC_OPEN_NAME = "eawrdmadmin_apikey@opendata.eawag.ch"


class Prod2Ext:
    """Move packages from ERIC to ERIC/open"""

    def __init__(
        self,
        # You need to be in the Eawag network to access ERIC
        source_host="https://data.eawag.ch",
        # addressing directly the app-server, circumventing NGINX
        target_host="http://eaw-ckan-ext2.eawag.wroot.emp-eaw.ch:5000",
    ):
        self.srchost, self.targethost = (source_host, target_host)
        self.srcapikey = get_secret(APIKEY_ERIC_NAME)
        self.targetapikey = get_secret(APIKEY_ERIC_OPEN_NAME)
        self.srcconn = self.connect(self.srchost, self.srcapikey)
        self.targetconn = self.connect(self.targethost, self.targetapikey)
        self.tmpdir = self.mktmpdir()
        self.hashtype = "sha256"

    def connect(self, host, apikey):
        conn = ckanapi.RemoteCKAN(host, apikey)
        return conn

    def mktmpdir(self, dn="./tmp"):
        try:
            os.mkdir(dn)
        except FileExistsError:
            pass
        return dn

    def getpkg(self, pkgname, conn):
        res = conn.call_action("package_show", data_dict={"id": pkgname})
        return res

    def getproject(self, projname, conn):
        res = conn.call_action("group_show", data_dict={"id": projname})
        return res

    def getuser(self, username, conn):
        rec = self.srcconn.call_action("user_show", data_dict={"id": username})
        return rec

    def get_citation(self, doi):
        if not doi:
            return None
        # distinguish DataCite DOI (more precisely: our prefix) from others
        # (those of a paper)
        if re.match("^10.25678", doi):
            print("\tRetrieving citation text from Datacite")
            # url = 'https://doi.org/{}'.format(doi)
            url = (
                f"https://api.datacite.org/dois/{doi}?style=american-geophysical-union"
            )
            # headers = {'Accept': 'text/x-bibliography; '
            #            'style=american-geophysical-union'}
            headers = {"Accept": "text/x-bibliography"}
        else:
            url = "https://doi.org/{}".format(doi)
            headers = {
                "Accept": "text/x-bibliography; " "style=american-geophysical-union"
            }

        r = requests.get(url, headers=headers, timeout=40)

        if not r.ok:
            print("Failed to get citation for DOI {}".format(doi))
            return None
        return r.text.encode(r.encoding).decode("utf-8")

    def plink_dora_from_doi(self, doi):
        dorabaseurl = "https://www.dora.lib4ri.ch/eawag/islandora/object/"
        doienc = re.sub(r"/", r"~slsh~", doi)
        doraquery = (
            "https://www.dora.lib4ri.ch/eawag/islandora/search/json/"
            "mods_identifier_doi_mt:({})".format(doienc)
        )
        r = requests.get(doraquery).json()
        if not r:
            print("WARNING: No DORA entry for DOI {}".format(doi))
            return None
        if len(r) > 1:
            print(
                "WARNING: multiple DORA records for one doi:\n" "DOI: {}\n".format(doi)
            )
        return "{}{}".format(dorabaseurl, r[0])

    def fix_publication_link(self, publication_link):
        print("Fixing publicationlink : {}".format(publication_link))
        if not publication_link:
            return {}
        elif re.search(r"lib4ri", publication_link):
            queryurl = os.path.join(publication_link, "datastream/MODS")
            record = requests.get(queryurl)
            root = ET.fromstring(record.text)
            ids = root.findall("{http://www.loc.gov/mods/v3}identifier")
            paper_dois = [i.text for i in ids if i.attrib["type"] == "doi"]
            if paper_dois:
                paper_doi = paper_dois[0]
                publicationlink = "https://doi.org/{}".format(paper_doi)
                return {
                    "publicationlink": publicationlink,
                    "publicationlink_dora": publication_link,
                    "paper_doi": paper_doi,
                }
            else:
                # use DORA-link
                return {
                    "publicationlink": None,
                    "publicationlink_dora": publication_link,
                    "paper_doi": None,
                }
        elif re.search(r"doi.org", publication_link):
            paper_doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", publication_link)
            return {
                "publicationlink": publication_link,
                "publicationlink_dora": self.plink_dora_from_doi(paper_doi),
                "paper_doi": paper_doi,
            }
        else:
            return {
                "publicationlink_url": publication_link,
                "publicationlink_dora": None,
                "paper_doi": None,
            }

    def mogrifypkg(self, pkg, doi, custom_citation_publication=None):
        print("\tMogifying package")
        maintainer_record = self.getuser(pkg["maintainer"], self.srcconn)
        usage_contact_record = self.getuser(pkg["usage_contact"], self.srcconn)
        usage_contact_target = (
            usage_contact_record["fullname"]
            + " <"
            + usage_contact_record["email"]
            + ">"
        )

        def _normalize_author(author):
            ## remove email addresses
            author = [re.sub(r"<.+@.+>", "", a).strip() for a in author]
            return author

        publinks = self.fix_publication_link(pkg.get("publicationlink"))
        print("publinks: {}".format(publinks))
        paper_doi = publinks.get("paper_doi")
        publicationlink = publinks.get("publicationlink")
        publicationlink_dora = publinks.get("publicationlink_dora")
        publicationlink_url = publinks.get("publicationlink_url")
        citation_publication = custom_citation_publication or self.get_citation(
            paper_doi
        )
        print(doi)
        pkg_update = {
            "isopen": True,
            "usage_contact": None,
            "url": "https://doi.org/{}/".format(doi),
            "doi": doi,
            "author": _normalize_author(pkg.get("author")),
            "reviewed_by": (
                None if pkg.get("review_level") in [None, "none"] else "redacted"
            ),
            "notes-2": None,
            "maintainer": maintainer_record["fullname"],
            "maintainer_email": usage_contact_target,
            "internal_id": pkg.get("id"),
            "id": None,
            "owner_org": pkg["organization"]["name"],
            "spatial": (
                pkg.get("spatial").strip() if pkg.get("spatial") else pkg.get("spatial")
            ),
            "resources": [],
            "citation": self.get_citation(doi),
            "paper_doi": paper_doi,
            "citation_publication": citation_publication,
            "publicationlink": publicationlink,
            "publicationlink_dora": publicationlink_dora,
            "publicationlink_url": publicationlink_url,
        }

        pkg.update(pkg_update)
        return pkg

    def mogrifyproject(self, prj):
        newproj = {}
        fields2copy = ["title", "description", "name"]
        for f in fields2copy:
            newproj[f] = prj[f]
        return newproj

    def download_resource(self, url, tmpfn):
        print("\t Downloading resources {}".format(url))
        getreq = requests.get(
            url, headers={"X-CKAN-API-Key": self.srcapikey}, stream=True
        )

        with open(tmpfn, "wb") as f:
            for chunk in getreq.iter_content(chunk_size=4096):
                f.write(chunk)
        return getreq.headers["Content-Type"]

    def create_dummy_resource(self, url, tmpfn):
        print("\t Creating dummy-resource for {}".format(url))
        with open(tmpfn, "w") as f:
            f.write("dummy")
        return "text/plain"

    def mogrifyresource(self, oldres):
        print("\n building resource record for {}".format(oldres["name"]))
        identical = ["name", "resource_type", "citation"]
        newres = {}
        for k in identical:
            try:
                newres[k] = oldres[k]
            except KeyError:
                print(
                    "field: {} not found in {} ({})\n".format(
                        k, oldres.get("name"), oldres.get("package_id")
                    )
                )
                if k == "citation":
                    print("Insering field citation with empty string\n")
                    newres[k] = ""

        if oldres.get("restricted_level") != "public":
            raise Exception("Resource {} is restricted. Aborting")

        newres["restricted_level"] = "public"
        newres["allowed_users"] = ""
        if oldres.get("publication") == "yes":
            newres["publication"] = "yes"
        print(newres)
        return newres

    def hash_resource(self, filename, resdir):
        hash_sha = hashlib.sha256()
        print("Calculating checksum for {} ...".format(filename))
        t0 = time.time()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_sha.update(chunk)
        digest = hash_sha.hexdigest()
        deltat = time.time() - t0
        print("\ttime: {} seconds".format(deltat))
        print("\t{}: {}".format("sha256", digest))
        resdir.update({"hashtype": self.hashtype, "hash": digest})

    def moveresources(
        self, pkg_in, skipsize=None, update_res=None, del_res=None, dummy_res=None
    ):
        pkgname = pkg_in["name"].split("/")[-1]
        print("Moving resources for package {}".format(pkgname))
        origpkg = self.getpkg(pkgname, self.srcconn)
        resources = origpkg.get("resources")
        print("len old resources: {}".format(len(resources)))

        def emptyhash(res):
            res["hash"] = ""
            res.pop("hashtype", None)
            return res

        # Create dummy-ressources
        if dummy_res:
            print("Dummy-resources to be uploaded: {}".format(dummy_res))
            resources = [emptyhash(r) for r in resources]

        # Update (move) just specific resources
        if update_res:
            print("Just moving (updating) ressources: {}".format(update_res))
            resources = [r for r in resources if r["name"] in update_res]
            if len(resources) != len(update_res):
                raise Exception("Something went wrong with specific updates")

        if del_res:
            print("Deleting resources: {}".format(pkgname, del_res))
            openpkg = self.targetconn.call_action("package_show", {"id": pkgname})
            openres = openpkg.get("resources", [])
            delids = [r["id"] for r in openres if r["name"] in del_res]
            print("Found resource ids to delete: {}".format(delids))
            for di in delids:
                self.targetconn.call_action("resource_delete", {"id": di})

        for oldres in resources:
            if skipsize is not None and oldres["size"] >= skipsize:
                print("Skipping {} in {}: too big".format(oldres["name"], pkgname))
                continue
            if oldres["name"] in pkg_in.get("exclude_resources", []):
                print("Skipping {} in {}: excluded".format(oldres["name"], pkgname))
                continue
            newres = self.mogrifyresource(oldres)
            newres["package_id"] = pkgname
            url = oldres.get("url")
            if oldres.get("url_type") == "upload":
                filename = os.path.basename(url)
                tmpfn = os.path.join(self.tmpdir, filename)
                if dummy_res and (oldres.get("name") in dummy_res):
                    contenttype = self.create_dummy_resource(url, tmpfn)
                else:
                    contenttype = self.download_resource(url, tmpfn)
                    self.hash_resource(tmpfn, newres)

                fileup = {"upload": (filename, open(tmpfn, "rb"), contenttype)}
                print("fileip: {}".format(fileup))
                newres.update(fileup)
                print("NEWRES: {}".format(newres))
                m = MultipartEncoder(newres)
                endpoint = self.targethost + "/api/3/action/resource_create"
                print("\tUploading resource {}".format(newres["name"]))
                postreq = requests.post(
                    endpoint,
                    data=m,
                    headers={
                        "Content-Type": m.content_type,
                        "X-CKAN-API-Key": self.targetapikey,
                    },
                )
                os.remove(tmpfn)
            else:
                print("\tLinking resource {}".format(newres["name"]))
                if pkg_in["rename_resources_url"]:
                    url = url.replace(
                        pkg_in["rename_resources_url"][0],
                        pkg_in["rename_resources_url"][1],
                        1,
                    )
                newres.update({"url": url})
                print(newres)
                self.targetconn.call_action("resource_create", data_dict=newres)

    def movepkg(self, pkg_in):
        pkgname = pkg_in["name"].split("/")[-1]
        doi = pkg_in.get("doi")
        custom_fields = pkg_in.get("custom_fields") or {}
        custom_citation_publication = pkg_in.get("custom_citation_publication")
        print("\nMoving package {}\nDOI: {}".format(pkgname, doi))
        origpkg = self.getpkg(pkgname, self.srcconn)
        origpkg.update(custom_fields)
        newpkg = self.mogrifypkg(origpkg, doi, custom_citation_publication)
        print(
            "ORIGPKG: publicationlink_dora: {}".format(
                origpkg.get("publicationlink_dora")
            )
        )
        print("\tCreating package")
        try:
            self.targetconn.call_action("package_show", data_dict={"id": pkgname})
        except ckanapi.NotFound:
            pass
        else:
            print("\n Package exists, deleting")
            self.targetconn.call_action("dataset_purge", data_dict={"id": pkgname})

        self.targetconn.call_action("package_create", data_dict=newpkg)

    def moveproject(self, projname):
        # Move basic project
        # Move image by hand!
        origprj = self.getproject(projname, self.srcconn)
        newprj = self.mogrifyproject(origprj)
        try:
            self.targetconn.call_action("group_show", data_dict={"id": projname})
        except ckanapi.NotFound:
            pass
        else:
            print("\n Project {} exists, deleting".format(projname))
            self.targetconn.call_action("group_purge", data_dict={"id": projname})
        print("\tCreating project {}".format(projname))
        self.targetconn.call_action("group_create", data_dict=newprj)

    def update_doi(self, pkgname, doi):
        """Inserts DOI and citation (retrieved from DataCite) into
        target-host metadata."""
        self.targetconn.call_action(
            "package_patch",
            data_dict={"id": pkgname, "doi": doi, "citation": self.get_citation(doi)},
        )

    # barely used
    def update_linked_resource_url(self, resid, url):
        self.targetconn.call_action(
            "resource_patch", data_dict={"id": resid, "url": url}
        )

    # Updates citation_info for associated paper
    # Retrieves citation-text from DataCite, if paper_doi is given.
    # Citation-text overridden if citation_publication is provided.
    def update_citation_info(self, pkgname, paper_doi=None, citation_publication=None):
        print("Updating citation info for {}".format(pkgname))
        pkg = self.targetconn.call_action("package_show", data_dict={"id": pkgname})
        if paper_doi:
            publicationlink = "https://doi.org/{}".format(paper_doi)
            print("New paper_doi: {}".format(paper_doi))
            print("New publikationlink: {}".format(publicationlink))
            pkg.update(
                {
                    "paper_doi": paper_doi,
                    "publicationlink": publicationlink,
                    "citation_publication": self.get_citation(paper_doi),
                }
            )
        if citation_publication:
            print("New citation_publication: {}".format(citation_publication))
            pkg.update({"citation_publication": citation_publication})

        self.targetconn.call_action("package_update", data_dict=pkg)

    # barely used
    def del_all_target_resources(self, pkgname):
        pkg = self.targetconn.call_action("package_show", data_dict={"id": pkgname})
        res_ids = [r["id"] for r in pkg["resources"]]
        for r in res_ids:
            print("deleting resource {} from {}.".format(r, pkg["name"]))
            self.targetconn.call_action("resource_delete", data_dict={"id": r})
