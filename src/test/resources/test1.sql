SELECT append_events(
               ARRAY ['Account@1']::TEXT[],
               0::BIGINT,
               ARRAY ['AccountOpened', 'AmountDeposited']::TEXT[],
               ARRAY [
                   '{"type": "AccountOpened", "id": 1}',
                   '{"type": "AmountDeposited", "amount": 100}'
                   ]::TEXT[]
       );