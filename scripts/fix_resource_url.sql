-- During the upload of large files originally they were uploaded with the addition '.empty_file.empty' and the renamed
-- Renaming does not change the url though. This results in the wrong filename when downloading the data.
-- This query will fix the url for all resource urls that end in '.empty_file.empty'
-- !!Afterwards the solr index must be re-indexed!!

UPDATE resource
SET url = substring(url from 1 for length(url) - length('.empty_file.empty'))
WHERE url LIKE '%.empty_file.empty';


-- This url will check if there are other urls with 'empty_file.empty' in it
SELECT * FROM resource WHERE url LIKE '%empty_file.empty';

