SELECT append_events(
               ARRAY ['Account@2']::TEXT[],
               2::BIGINT,
               ARRAY ['AccountOpened']::TEXT[],
               ARRAY [
                   '{"type": "AccountOpened", "id": 2}'
                   ]::TEXT[]
       );