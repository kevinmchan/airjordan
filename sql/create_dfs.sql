create table if not exists public.dfs
(
    source text,
    slate_minGameStart timestamp with time zone,
    slate_date timestamp with time zone,
    slate_id text,
    label text,
    player_id bigint,
    rosterSlots text[]
);