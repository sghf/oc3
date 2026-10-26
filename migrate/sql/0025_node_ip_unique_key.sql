-- 2026-10-26: a unique key on node_ip, an address of a node: its interface
-- and its address. The feed of the node writes its addresses with an insert
-- or update on the unique key, and node_ip had none: no address was found
-- again, each feed inserted them all anew, consuming as many auto-increment
-- values, and deleted the former rows as outdated.
--
-- The duplicates go first, the latest row of each address kept: the key
-- cannot be created over them. A data statement before the DDL, as in 0020:
-- run again, the deletion has nothing left to delete.
DELETE older FROM node_ip older
  JOIN node_ip newer ON newer.node_id = older.node_id AND newer.intf <=> older.intf
    AND newer.addr <=> older.addr AND newer.id > older.id;
ALTER TABLE node_ip ADD UNIQUE INDEX IF NOT EXISTS uk_node_intf_addr (node_id, intf, addr);
