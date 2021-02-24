create table if not exists public.gamelines
(
    game_id bigint,
    source text,
    moneyLines json,
    pointSpreads json
);