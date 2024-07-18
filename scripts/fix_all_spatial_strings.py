import json
import time

import tqdm

from ckool.ckan.ckan import CKAN

if __name__ == "__main__":
    ckan = CKAN(
        server="https://data.eawag.ch",
        token="...",
        verify_certificate=True,
    )

    packages = ckan.get_all_packages()["results"]

    to_fix = []
    for pkg in packages:
        if pkg["spatial"] == "{}":
            to_fix.append({"id": pkg["id"], "name": pkg["name"], "spatial": pkg["spatial"]})
    len_fixes = len(to_fix)
    for i, fix in enumerate(to_fix):
        try:
            print(f"{i}/{len_fixes}", fix)
            time.sleep(0.3)
            ckan.patch_package_metadata(
                package_id=fix["id"], data={"spatial": ""}
            )

        except Exception as e:
            print(fix)
            raise

    print("FIXED")
    print(to_fix)



