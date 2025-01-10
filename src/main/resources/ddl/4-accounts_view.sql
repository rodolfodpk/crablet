\connect postgres ;

INSERT INTO subscriptions (name, sequence_id)
values ('accounts-view', 0);

CREATE TABLE accounts_view
(
    id      INT NOT NULL PRIMARY KEY,
    balance INT NOT NULL
);
