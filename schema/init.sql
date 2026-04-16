-- =============================================================================
-- FIX 4.4 Protocol Log Analyzer — MySQL Schema
-- =============================================================================
-- Usage:
--   mysql -u root -p < schema/init.sql
--
-- Creates the database (if not exists) and the fix_messages table with all
-- relevant FIX 4.4 fields and performance indexes.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS fix_analyzer
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE fix_analyzer;

-- ---------------------------------------------------------------------------
-- fix_messages: one row per decoded FIX message
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fix_messages (
    id               BIGINT UNSIGNED    NOT NULL AUTO_INCREMENT,

    -- Session-level fields
    msg_type         VARCHAR(2)         NOT NULL COMMENT 'Tag 35 — D=NewOrder, 8=ExecRpt, 9=CxlRej, A=Logon, 5=Logout, 3=Reject',
    msg_type_name    VARCHAR(50)        NOT NULL DEFAULT '' COMMENT 'Human-readable MsgType label',
    sender_comp_id   VARCHAR(50)        NOT NULL DEFAULT '' COMMENT 'Tag 49',
    target_comp_id   VARCHAR(50)        NOT NULL DEFAULT '' COMMENT 'Tag 56',
    msg_seq_num      INT UNSIGNED       NOT NULL DEFAULT 0  COMMENT 'Tag 34 — used for gap detection',

    -- Timing
    sending_time     DATETIME(6)        NOT NULL             COMMENT 'Tag 52 — microsecond precision',

    -- Instrument
    symbol           VARCHAR(10)        NOT NULL DEFAULT ''  COMMENT 'Tag 55',

    -- Order fields
    side             CHAR(1)            NULL                 COMMENT 'Tag 54 — 1=Buy 2=Sell',
    order_qty        DECIMAL(15, 2)     NULL                 COMMENT 'Tag 38',
    price            DECIMAL(15, 4)     NULL                 COMMENT 'Tag 44',

    -- Execution fields
    avg_px           DECIMAL(15, 4)     NULL                 COMMENT 'Tag 6',
    cum_qty          DECIMAL(15, 2)     NULL                 COMMENT 'Tag 14',
    leaves_qty       DECIMAL(15, 2)     NULL                 COMMENT 'Tag 151',
    ord_status       CHAR(1)            NULL                 COMMENT 'Tag 39 — 0=New 1=PartFill 2=Fill 8=Reject',
    exec_type        CHAR(1)            NULL                 COMMENT 'Tag 150 — 0=New 1=PartFill 2=Fill 8=Reject F=Trade',

    -- Identifiers
    cl_ord_id        VARCHAR(50)        NOT NULL DEFAULT ''  COMMENT 'Tag 11 — client order ID',
    order_id         VARCHAR(50)        NOT NULL DEFAULT ''  COMMENT 'Tag 37 — exchange order ID',
    exec_id          VARCHAR(50)        NOT NULL DEFAULT ''  COMMENT 'Tag 17 — execution report ID',

    -- Rejection / free text
    text             VARCHAR(255)       NOT NULL DEFAULT ''  COMMENT 'Tag 58 — rejection reason or free text',

    -- Raw message storage
    raw_message      TEXT               NOT NULL             COMMENT 'Full raw FIX message with | as SOH delimiter',

    -- Ingestion metadata
    created_at       DATETIME           NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Row insertion timestamp',

    PRIMARY KEY (id)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Decoded FIX 4.4 messages — one row per message';

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- Symbol lookups (price history, order book queries)
CREATE INDEX idx_symbol
    ON fix_messages (symbol);

-- Time-range queries (latency analysis, dashboards)
CREATE INDEX idx_sending_time
    ON fix_messages (sending_time);

-- MsgType filtering (find all rejections, execution reports, etc.)
CREATE INDEX idx_msg_type
    ON fix_messages (msg_type);

-- Sequence gap detection — per-session ordered sequence
CREATE INDEX idx_seq_gap
    ON fix_messages (sender_comp_id, msg_seq_num);

-- Order lifecycle tracking — join NewOrder → fills → cancels by ClOrdID
CREATE INDEX idx_cl_ord_id
    ON fix_messages (cl_ord_id);

-- Composite: symbol + time — common analytical query pattern
CREATE INDEX idx_symbol_time
    ON fix_messages (symbol, sending_time);

-- Composite: msg_type + ord_status — rejection rate queries
CREATE INDEX idx_type_status
    ON fix_messages (msg_type, ord_status);

-- ---------------------------------------------------------------------------
-- Optional: a view for quick rejection review
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_rejections AS
    SELECT
        id,
        sending_time,
        symbol,
        side,
        cl_ord_id,
        order_id,
        text          AS rejection_reason,
        msg_seq_num,
        sender_comp_id
    FROM fix_messages
    WHERE msg_type = '8'
      AND ord_status = '8'
    ORDER BY sending_time DESC;

-- ---------------------------------------------------------------------------
-- Optional: a view for sequence gap analysis
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_sequence_gaps AS
    SELECT
        sender_comp_id,
        msg_seq_num,
        LAG(msg_seq_num) OVER (
            PARTITION BY sender_comp_id
            ORDER BY msg_seq_num
        ) AS prev_seq_num,
        msg_seq_num - LAG(msg_seq_num) OVER (
            PARTITION BY sender_comp_id
            ORDER BY msg_seq_num
        ) - 1 AS gap_size,
        sending_time
    FROM fix_messages
    WHERE msg_type NOT IN ('A', '5')   -- exclude Logon/Logout
    HAVING gap_size > 0;
