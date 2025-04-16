# Ensures that the user has locally the db dir in /tmp.

curl -L https://raw.githubusercontent.com/dojoengine/dojo/6300eb467bb245e2455f31846f7cacf213dbf9e0/spawn-and-move-db.tar.gz -o spawn-and-move-db.tar.gz
curl -L https://raw.githubusercontent.com/dojoengine/dojo/6300eb467bb245e2455f31846f7cacf213dbf9e0/types-test-db.tar.gz -o types-test-db.tar.gz
rm -rf /tmp/spawn-and-move-db
rm -rf /tmp/types-test-db
tar xzf spawn-and-move-db.tar.gz -C /tmp/
tar xzf types-test-db.tar.gz -C /tmp/

# Ensures the spawn and move example is available.

