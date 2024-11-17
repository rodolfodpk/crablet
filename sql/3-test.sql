SELECT append_events(
               ARRAY ['domain_1@123', 'domain_2@123']::TEXT[], -- domain IDs
               0::BIGINT, -- based sequence ID
               ARRAY ['UserRegistered', 'UserUpdated', 'UserDeleted', 'PasswordChanged', 'UserProfileUpdated']::TEXT[], -- event types
               ARRAY [
                   '{"type": "UserRegistered", "user_id": "123", "name": "John Doe", "email": "john@example.com"}',
                   '{"type": "UserUpdated", "user_id": "123", "name": "John Doe Updated", "email": "john.new@example.com"}',
                   '{"type": "UserDeleted", "user_id": "124", "name": "Jane Doe", "email": "jane@example.com"}',
                   '{"type": "PasswordChanged", "user_id": "123", "password": "newpassword123"}',
                   '{"type": "UserProfileUpdated", "user_id": "123", "profile": {"age": 30, "city": "New York"}}'
                   ]::TEXT[] -- event payloads (as JSON string)
       ) AS last_sequence_id;
