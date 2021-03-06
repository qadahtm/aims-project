create table blocked_transactions_table
(transaction_id xid, detection_time_stamp timestamp) without oids;

create table malicious_transactions_table
(transaction_id xid, detection_time_stamp timestamp) without oids;

CREATE OR REPLACE FUNCTION alertMTxn(etxid bigint)
    RETURNS void AS $$
DECLARE
    dummy xid := etxid;
    ts_current timestamp := current_timestamp;
BEGIN
  insert into malicious_transactions_table(transaction_id, detection_time_stamp) 
        values (dummy, ts_current);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION blockTxn(etxid bigint)
    RETURNS void AS $$
DECLARE
    dummy xid := etxid;
    ts_current timestamp := current_timestamp;
BEGIN
  insert into blocked_transactions_table values (dummy, ts_current);
END;
$$ LANGUAGE plpgsql;
