# Mongo Transaction Concurrent

Demo app to test mongo transactions.

This demo is a simplification of how events would be processed by axon, focussing only on the mongo transaction part.
Events are created and processed from multiple threads, since they always update the 'tracker' in the projection, this triggers a lot of 'error 112 (WriteConflict)' errors.
The demo proves this is no problem, as the token updates part of the transaction are rolled back, and at the end the amount of documents in the projection is equal to the amount of events +1.