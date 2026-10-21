-- 2026-10-21: the notes of the nodes and services (GET/POST /nodes/{node_id}/wiki,
-- /services/{svc_id}/wiki) read the revisions of a page by name, its node_id or
-- svc_id, the latest first: an index leading with the name. The table, MyISAM
-- as the legacy collector made it, moves to InnoDB, for a save to lock only the
-- rows of its page while it checks that it edits the latest revision.
ALTER TABLE wiki_pages ENGINE=InnoDB;
ALTER TABLE wiki_pages ADD INDEX IF NOT EXISTS k_name_id (name, id);
