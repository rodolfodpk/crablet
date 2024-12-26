SELECT append_events(
               ARRAY ['Account@1','Account@2']::TEXT[],
               3::BIGINT,
               ARRAY ['AmountTransferred']::TEXT[],
               ARRAY [
                   '{"type": "AmountTransferred", "fromAcct": 1, "toAcct": 2, "amount": 30}'
                   ]::TEXT[]
       );