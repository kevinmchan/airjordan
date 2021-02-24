create table if not exists public.playbyplay
(
    game_id bigint,
    description text,
    playType text,
    playStatus json,
    play json
);