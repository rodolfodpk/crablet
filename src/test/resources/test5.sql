SELECT append_events(
               ARRAY ['Account@1','Account@2']::TEXT[],
               5::BIGINT,
               ARRAY ['AmountTransferred']::TEXT[],
               ARRAY [
                   '{"type": "AmountTransferred", "fromAcct": 2, "toAcct": 1, "amount": 1}'
                   ]::TEXT[]
       );