{
  description = "Bridge-DB test environment — all 8 database providers with seed data";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            redis
            mongodb
            sqlite
            postgresql
            mysql80
            mariadb
            cockroachdb
            jq
            go
          ];

          shellHook = ''
                        export SEED_DIR="$PWD/.seed-data"
                        export REDIS_DIR="$SEED_DIR/redis"
                        export REDIS2_DIR="$SEED_DIR/redis2"
                        export MONGO_DIR="$SEED_DIR/mongodb"
                        export MONGO2_DIR="$SEED_DIR/mongodb2"
                        export PGDATA="$SEED_DIR/postgres"
                        export PGDATA2="$SEED_DIR/postgres2"
                        export MYSQL_DIR="$SEED_DIR/mysql"
                        export MYSQL2_DIR="$SEED_DIR/mysql2"
                        export MYSQL_SOCK_DIR="/tmp"
            export MYSQL_SOCK="bridge-test-mysql.sock"
                        export MYSQL2_SOCK_DIR="/tmp"
            export MYSQL2_SOCK="bridge-test-mysql2.sock"
                        export MARIA_DIR="$SEED_DIR/mariadb"
                        export MARIA2_DIR="$SEED_DIR/mariadb2"
                        export MARIA_SOCK_DIR="/tmp"
            export MARIA_SOCK="bridge-test-maria.sock"
                        export MARIA2_SOCK_DIR="/tmp"
            export MARIA2_SOCK="bridge-test-maria2.sock"
                        export CRDB_DIR="$SEED_DIR/cockroachdb"
                        export CRDB2_DIR="$SEED_DIR/cockroachdb2"
                        export SQLITE_DB="$SEED_DIR/test.db"
                        export MYSQL_BIN="${pkgs.mysql80}/bin"
                        export MARIA_BIN="${pkgs.mariadb}/bin"

                        mkdir -p "$SEED_DIR"

                        echo ""
                        echo "=== Bridge-DB Test Environment ==="
                        echo ""
                        echo "  Redis (primary)       redis://localhost:6379"
                        echo "  Redis (secondary)     redis://localhost:6380"
                        echo ""
                        echo "  MongoDB (primary)     mongodb://localhost:27017/testdb"
                        echo "  MongoDB (secondary)   mongodb://localhost:27018/testdb"
                        echo ""
                        echo "  SQLite                sqlite://$SQLITE_DB"
                        echo ""
                        echo "  PostgreSQL (primary)  postgresql://127.0.0.1:5432/testdb?sslmode=disable"
                        echo "  PostgreSQL (secondary) postgresql://127.0.0.1:5433/testdb?sslmode=disable"
                        echo ""
                        echo "  MySQL (primary)       mysql://root@127.0.0.1:3306/testdb"
                        echo "  MySQL (secondary)     mysql://root@127.0.0.1:3307/testdb"
                        echo ""
                        echo "  MariaDB (primary)     mariadb://root@127.0.0.1:3308/testdb"
                        echo "  MariaDB (secondary)   mariadb://root@127.0.0.1:3309/testdb"
                        echo ""
                        echo "  CockroachDB (primary) cockroachdb://root@localhost:26257/testdb?sslmode=disable"
                        echo "  CockroachDB (secondary) cockroachdb://root@localhost:26258/testdb?sslmode=disable"
                        echo ""
                        echo "  NOTE: MSSQL requires Docker. Run:"
                        echo "    docker run -d --name mssql -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=BridgeDb123!' -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest"
                        echo "    docker run -d --name mssql2 -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=BridgeDb123!' -p 1434:1433 mcr.microsoft.com/mssql/server:2022-latest"
                        echo "    MSSQL URL: mssql://sa:BridgeDb123!@localhost:1433/testdb"
                        echo ""
                        echo "-----------------------------------------"
                        echo ""

                        # ── Auto-start servers ──────────────────────────────────────

                        # Redis (primary)
                        if ! redis-cli ping > /dev/null 2>&1; then
                          mkdir -p "$REDIS_DIR"
                          redis-server --dir "$REDIS_DIR" --daemonize yes --loglevel warning
                          echo "[auto] Redis (primary) started on port 6379"
                        else
                          echo "[auto] Redis (primary) already running"
                        fi

                        # Redis (secondary)
                        if ! redis-cli -p 6380 ping > /dev/null 2>&1; then
                          mkdir -p "$REDIS2_DIR"
                          redis-server --port 6380 --dir "$REDIS2_DIR" --daemonize yes --loglevel warning
                          echo "[auto] Redis (secondary) started on port 6380"
                        else
                          echo "[auto] Redis (secondary) already running"
                        fi

                        # MongoDB (primary)
                        if ! mongo --quiet --eval "db.runCommand({ping:1})" > /dev/null 2>&1; then
                          mkdir -p "$MONGO_DIR"
                          mongod --dbpath "$MONGO_DIR" --fork --logpath "$MONGO_DIR/mongod.log" > /dev/null 2>&1
                          echo "[auto] MongoDB (primary) started on port 27017"
                        else
                          echo "[auto] MongoDB (primary) already running"
                        fi

                        # MongoDB (secondary)
                        if ! mongo --quiet --port 27018 --eval "db.runCommand({ping:1})" > /dev/null 2>&1; then
                          mkdir -p "$MONGO2_DIR"
                          mongod --dbpath "$MONGO2_DIR" --port 27018 --fork --logpath "$MONGO2_DIR/mongod.log" > /dev/null 2>&1
                          echo "[auto] MongoDB (secondary) started on port 27018"
                        else
                          echo "[auto] MongoDB (secondary) already running"
                        fi

                        # PostgreSQL (primary)
                        if ! pg_isready -h /tmp -p 5432 > /dev/null 2>&1; then
                          if [ ! -d "$PGDATA" ]; then
                            initdb "$PGDATA" --auth=trust --no-locale --encoding=UTF8 > /dev/null 2>&1
                          fi
                          pg_ctl -D "$PGDATA" -l "$PGDATA/server.log" -o "-p 5432 -k /tmp" start > /dev/null 2>&1
                          sleep 1
                          createdb -h /tmp testdb 2>/dev/null || true
                          echo "[auto] PostgreSQL (primary) started on port 5432"
                        else
                          echo "[auto] PostgreSQL (primary) already running"
                        fi

                        # PostgreSQL (secondary)
                        if ! pg_isready -h /tmp -p 5433 > /dev/null 2>&1; then
                          if [ ! -d "$PGDATA2" ]; then
                            initdb "$PGDATA2" --auth=trust --no-locale --encoding=UTF8 > /dev/null 2>&1
                          fi
                          pg_ctl -D "$PGDATA2" -l "$PGDATA2/server.log" -o "-p 5433 -k /tmp" start > /dev/null 2>&1
                          sleep 1
                          createdb -h /tmp -p 5433 testdb 2>/dev/null || true
                          echo "[auto] PostgreSQL (secondary) started on port 5433"
                        else
                          echo "[auto] PostgreSQL (secondary) already running"
                        fi

                        # MySQL (primary)
                        if ! $MYSQL_BIN/mysqladmin ping -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" --silent 2>/dev/null; then
                          if [ ! -d "$MYSQL_DIR/mysql" ]; then
                            rm -rf "$MYSQL_DIR"
                            mkdir -p "$MYSQL_DIR"
                            $MYSQL_BIN/mysqld --initialize-insecure --user=$USER --datadir="$MYSQL_DIR" > /dev/null 2>&1
                          fi
                          $MYSQL_BIN/mysqld --user=$USER --datadir="$MYSQL_DIR" --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" --port=3306 --bind-address=127.0.0.1 --pid-file="$MYSQL_DIR/mysql.pid" --daemonize > /dev/null 2>&1
                          sleep 2
                          if $MYSQL_BIN/mysqladmin ping -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" --silent 2>/dev/null; then
                            $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" -e "CREATE DATABASE IF NOT EXISTS testdb;" 2>/dev/null
                            echo "[auto] MySQL (primary) started on port 3306"
                          else
                            echo "[auto] MySQL (primary) failed to start"
                          fi
                        else
                          echo "[auto] MySQL (primary) already running"
                        fi

                        # MySQL (secondary)
                        if ! $MYSQL_BIN/mysqladmin ping -u root --socket="$MYSQL2_SOCK_DIR/$MYSQL2_SOCK" --silent 2>/dev/null; then
                          if [ ! -d "$MYSQL2_DIR/mysql" ]; then
                            rm -rf "$MYSQL2_DIR"
                            mkdir -p "$MYSQL2_DIR"
                            $MYSQL_BIN/mysqld --initialize-insecure --user=$USER --datadir="$MYSQL2_DIR" > /dev/null 2>&1
                          fi
                          $MYSQL_BIN/mysqld --user=$USER --datadir="$MYSQL2_DIR" --socket="$MYSQL2_SOCK_DIR/$MYSQL2_SOCK" --port=3307 --bind-address=127.0.0.1 --pid-file="$MYSQL2_DIR/mysql.pid" --daemonize > /dev/null 2>&1
                          sleep 2
                          if $MYSQL_BIN/mysqladmin ping -u root --socket="$MYSQL2_SOCK_DIR/$MYSQL2_SOCK" --silent 2>/dev/null; then
                            $MYSQL_BIN/mysql -u root --socket="$MYSQL2_SOCK_DIR/$MYSQL2_SOCK" -e "CREATE DATABASE IF NOT EXISTS testdb;" 2>/dev/null
                            echo "[auto] MySQL (secondary) started on port 3307"
                          else
                            echo "[auto] MySQL (secondary) failed to start"
                          fi
                        else
                          echo "[auto] MySQL (secondary) already running"
                        fi

                        # MariaDB (primary)
                        if ! $MARIA_BIN/mysqladmin ping -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" --silent 2>/dev/null; then
                          if [ ! -d "$MARIA_DIR/mysql" ]; then
                            rm -rf "$MARIA_DIR"
                            mkdir -p "$MARIA_DIR"
                            $MARIA_BIN/mariadb-install-db --user=$USER --datadir="$MARIA_DIR" --auth-root-authentication-method=normal > /dev/null 2>&1
                          fi
                          $MARIA_BIN/mariadbd --defaults-file=/dev/null --user=$USER --datadir="$MARIA_DIR" --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" --port=3308 --bind-address=127.0.0.1 --pid-file="$MARIA_DIR/mariadb.pid" --daemonize > /dev/null 2>&1
                          sleep 2
                          if $MARIA_BIN/mysqladmin ping -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" --silent 2>/dev/null; then
                            $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" -e "CREATE DATABASE IF NOT EXISTS testdb;" 2>/dev/null
                            echo "[auto] MariaDB (primary) started on port 3308"
                          else
                            echo "[auto] MariaDB (primary) failed to start"
                          fi
                        else
                          echo "[auto] MariaDB (primary) already running"
                        fi

                        # MariaDB (secondary)
                        if ! $MARIA_BIN/mysqladmin ping -u root --socket="$MARIA2_SOCK_DIR/$MARIA2_SOCK" --silent 2>/dev/null; then
                          if [ ! -d "$MARIA2_DIR/mysql" ]; then
                            rm -rf "$MARIA2_DIR"
                            mkdir -p "$MARIA2_DIR"
                            $MARIA_BIN/mariadb-install-db --user=$USER --datadir="$MARIA2_DIR" --auth-root-authentication-method=normal > /dev/null 2>&1
                          fi
                          $MARIA_BIN/mariadbd --defaults-file=/dev/null --user=$USER --datadir="$MARIA2_DIR" --socket="$MARIA2_SOCK_DIR/$MARIA2_SOCK" --port=3309 --bind-address=127.0.0.1 --pid-file="$MARIA2_DIR/mariadb.pid" --daemonize > /dev/null 2>&1
                          sleep 2
                          if $MARIA_BIN/mysqladmin ping -u root --socket="$MARIA2_SOCK_DIR/$MARIA2_SOCK" --silent 2>/dev/null; then
                            $MARIA_BIN/mariadb -u root --socket="$MARIA2_SOCK_DIR/$MARIA2_SOCK" -e "CREATE DATABASE IF NOT EXISTS testdb;" 2>/dev/null
                            echo "[auto] MariaDB (secondary) started on port 3309"
                          else
                            echo "[auto] MariaDB (secondary) failed to start"
                          fi
                        else
                          echo "[auto] MariaDB (secondary) already running"
                        fi

                        # CockroachDB (primary)
                        if ! cockroach sql --host=localhost --port=26257 --insecure -e "SELECT 1" > /dev/null 2>&1; then
                          mkdir -p "$CRDB_DIR"
                          cockroach start-single-node --insecure --store=path="$CRDB_DIR" --listen-addr=localhost:26257 --http-addr=localhost:8079 --background > "$CRDB_DIR/cockroach.log" 2>&1
                          crdb_ready=0
                          for i in $(seq 1 30); do
                            if cockroach sql --host=localhost --port=26257 --insecure -e "SELECT 1" > /dev/null 2>&1; then
                              crdb_ready=1
                              break
                            fi
                            sleep 1
                          done
                          if [ "$crdb_ready" -eq 1 ]; then
                            cockroach sql --host=localhost --port=26257 --insecure -e "CREATE DATABASE IF NOT EXISTS testdb;" > /dev/null 2>&1
                            echo "[auto] CockroachDB (primary) started on port 26257"
                          else
                            echo "[auto] CockroachDB (primary) failed to become ready; see $CRDB_DIR/cockroach.log"
                          fi
                        else
                          echo "[auto] CockroachDB (primary) already running"
                        fi

                        # CockroachDB (secondary)
                        if ! cockroach sql --host=localhost --port=26258 --insecure -e "SELECT 1" > /dev/null 2>&1; then
                          mkdir -p "$CRDB2_DIR"
                          cockroach start-single-node --insecure --store=path="$CRDB2_DIR" --listen-addr=localhost:26258 --http-addr=localhost:8081 --background > "$CRDB2_DIR/cockroach.log" 2>&1
                          crdb_ready=0
                          for i in $(seq 1 30); do
                            if cockroach sql --host=localhost --port=26258 --insecure -e "SELECT 1" > /dev/null 2>&1; then
                              crdb_ready=1
                              break
                            fi
                            sleep 1
                          done
                          if [ "$crdb_ready" -eq 1 ]; then
                            cockroach sql --host=localhost --port=26258 --insecure -e "CREATE DATABASE IF NOT EXISTS testdb;" > /dev/null 2>&1
                            echo "[auto] CockroachDB (secondary) started on port 26258"
                          else
                            echo "[auto] CockroachDB (secondary) failed to become ready; see $CRDB2_DIR/cockroach.log"
                          fi
                        else
                          echo "[auto] CockroachDB (secondary) already running"
                        fi

                        echo ""

                        # ── Seed test data ─────────────────────────────────────────

                        # === SQLite ===
                        sqlite3 "$SQLITE_DB" "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL, age INTEGER);"
                        sqlite3 "$SQLITE_DB" "CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, category TEXT NOT NULL, price REAL NOT NULL, stock INTEGER DEFAULT 0);"
                        sqlite3 "$SQLITE_DB" "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL DEFAULT 1, total_price REAL, created_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (user_id) REFERENCES users(id), FOREIGN KEY (product_id) REFERENCES products(id));"
                        sqlite3 "$SQLITE_DB" "INSERT OR IGNORE INTO users (name, email, age) VALUES ('Alice Johnson','alice@example.com',30), ('Bob Smith','bob@example.com',25), ('Charlie Brown','charlie@example.com',35), ('Diana Prince','diana@example.com',28), ('Eve Adams','eve@example.com',22), ('Frank Miller','frank@example.com',40), ('Grace Lee','grace@example.com',33), ('Henry Wilson','henry@example.com',27), ('Irene Davis','irene@example.com',31), ('Jack Taylor','jack@example.com',29), ('Karen White','karen@example.com',36), ('Leo Garcia','leo@example.com',24), ('Mia Martinez','mia@example.com',26), ('Nathan Clark','nathan@example.com',38), ('Olivia Lewis','olivia@example.com',23), ('Paul Walker','paul@example.com',34), ('Quinn Hall','quinn@example.com',21), ('Rachel Young','rachel@example.com',32), ('Sam King','sam@example.com',37), ('Tina Wright','tina@example.com',28), ('Uma Scott','uma@example.com',26), ('Victor Green','victor@example.com',41), ('Wendy Baker','wendy@example.com',30), ('Xavier Adams','xavier@example.com',35), ('Yara Collins','yara@example.com',29);"
                        sqlite3 "$SQLITE_DB" "INSERT OR IGNORE INTO products (name, category, price, stock) VALUES ('Laptop','Electronics',999.99,50), ('Wireless Mouse','Electronics',29.99,200), ('Mechanical Keyboard','Electronics',79.99,150), ('Monitor 27 inch','Electronics',399.99,30), ('Noise-Cancelling Headphones','Electronics',149.99,75), ('Office Chair','Furniture',249.99,40), ('Standing Desk','Furniture',499.99,20), ('Desk Lamp','Furniture',39.99,120), ('Bookshelf','Furniture',129.99,55), ('Filing Cabinet','Furniture',89.99,65), ('Python Cookbook','Books',44.99,300), ('Design Patterns','Books',54.99,250), ('Clean Code','Books',39.99,400), ('The Pragmatic Programmer','Books',49.99,180), ('Refactoring','Books',42.99,220), ('Running Shoes','Sports',119.99,90), ('Yoga Mat','Sports',24.99,500), ('Dumbbell Set','Sports',79.99,60), ('Resistance Bands','Sports',19.99,800), ('Jump Rope','Sports',14.99,650), ('Coffee Maker','Kitchen',89.99,110), ('Blender','Kitchen',59.99,95), ('Toaster','Kitchen',34.99,130), ('Knife Set','Kitchen',69.99,70), ('Water Bottle','Kitchen',12.99,1000);"
                        sqlite3 "$SQLITE_DB" "INSERT OR IGNORE INTO orders (user_id, product_id, quantity, total_price) VALUES (1,1,1,999.99), (1,3,1,79.99), (2,2,2,59.98), (3,5,1,149.99), (4,4,1,399.99), (5,2,3,89.97), (5,3,1,79.99), (6,6,1,249.99), (7,7,1,499.99), (8,8,2,79.98), (9,11,1,44.99), (9,12,1,54.99), (10,16,1,119.99), (11,17,3,74.97), (12,21,1,89.99), (13,22,1,59.99), (14,13,2,79.98), (15,9,1,129.99), (16,14,1,49.99), (17,18,2,39.98), (18,10,1,89.99), (19,15,1,42.99), (20,19,4,79.96), (21,23,1,34.99), (22,20,2,29.98), (23,24,1,69.99), (24,25,5,64.95), (25,1,1,999.99), (1,5,2,299.98), (3,11,1,44.99), (6,14,1,49.99), (8,17,1,24.99), (10,21,1,89.99);"
                        echo "[seed] SQLite: users, products, orders"

                        # === Redis ===
                        if redis-cli ping > /dev/null 2>&1; then
                          redis-cli HSET user:1 name "Alice Johnson" email "alice@example.com" age 30
                          redis-cli HSET user:2 name "Bob Smith" email "bob@example.com" age 25
                          redis-cli HSET user:3 name "Charlie Brown" email "charlie@example.com" age 35
                          redis-cli HSET user:4 name "Diana Prince" email "diana@example.com" age 28
                          redis-cli HSET user:5 name "Eve Adams" email "eve@example.com" age 22
                          redis-cli HSET user:6 name "Frank Miller" email "frank@example.com" age 40
                          redis-cli HSET user:7 name "Grace Lee" email "grace@example.com" age 33
                          redis-cli HSET user:8 name "Henry Wilson" email "henry@example.com" age 27
                          redis-cli HSET user:9 name "Irene Davis" email "irene@example.com" age 31
                          redis-cli HSET user:10 name "Jack Taylor" email "jack@example.com" age 29
                          redis-cli HSET user:11 name "Karen White" email "karen@example.com" age 36
                          redis-cli HSET user:12 name "Leo Garcia" email "leo@example.com" age 24
                          redis-cli HSET user:13 name "Mia Martinez" email "mia@example.com" age 26
                          redis-cli HSET user:14 name "Nathan Clark" email "nathan@example.com" age 38
                          redis-cli HSET user:15 name "Olivia Lewis" email "olivia@example.com" age 23
                          redis-cli HSET user:16 name "Paul Walker" email "paul@example.com" age 34
                          redis-cli HSET user:17 name "Quinn Hall" email "quinn@example.com" age 21
                          redis-cli HSET user:18 name "Rachel Young" email "rachel@example.com" age 32
                          redis-cli HSET user:19 name "Sam King" email "sam@example.com" age 37
                          redis-cli HSET user:20 name "Tina Wright" email "tina@example.com" age 28
                          redis-cli HSET user:21 name "Uma Scott" email "uma@example.com" age 26
                          redis-cli HSET user:22 name "Victor Green" email "victor@example.com" age 41
                          redis-cli HSET user:23 name "Wendy Baker" email "wendy@example.com" age 30
                          redis-cli HSET user:24 name "Xavier Adams" email "xavier@example.com" age 35
                          redis-cli HSET user:25 name "Yara Collins" email "yara@example.com" age 29
                          redis-cli SET product:1 '{"name":"Laptop","category":"Electronics","price":999.99,"stock":50}'
                          redis-cli SET product:2 '{"name":"Wireless Mouse","category":"Electronics","price":29.99,"stock":200}'
                          redis-cli SET product:3 '{"name":"Mechanical Keyboard","category":"Electronics","price":79.99,"stock":150}'
                          redis-cli SET product:4 '{"name":"Monitor 27 inch","category":"Electronics","price":399.99,"stock":30}'
                          redis-cli SET product:5 '{"name":"Noise-Cancelling Headphones","category":"Electronics","price":149.99,"stock":75}'
                          redis-cli SET product:6 '{"name":"Office Chair","category":"Furniture","price":249.99,"stock":40}'
                          redis-cli SET product:7 '{"name":"Standing Desk","category":"Furniture","price":499.99,"stock":20}'
                          redis-cli SET product:8 '{"name":"Desk Lamp","category":"Furniture","price":39.99,"stock":120}'
                          redis-cli SET product:9 '{"name":"Bookshelf","category":"Furniture","price":129.99,"stock":55}'
                          redis-cli SET product:10 '{"name":"Filing Cabinet","category":"Furniture","price":89.99,"stock":65}'
                          redis-cli SET product:11 '{"name":"Python Cookbook","category":"Books","price":44.99,"stock":300}'
                          redis-cli SET product:12 '{"name":"Design Patterns","category":"Books","price":54.99,"stock":250}'
                          redis-cli SET product:13 '{"name":"Clean Code","category":"Books","price":39.99,"stock":400}'
                          redis-cli SET product:14 '{"name":"The Pragmatic Programmer","category":"Books","price":49.99,"stock":180}'
                          redis-cli SET product:15 '{"name":"Refactoring","category":"Books","price":42.99,"stock":220}'
                          redis-cli SET product:16 '{"name":"Running Shoes","category":"Sports","price":119.99,"stock":90}'
                          redis-cli SET product:17 '{"name":"Yoga Mat","category":"Sports","price":24.99,"stock":500}'
                          redis-cli SET product:18 '{"name":"Dumbbell Set","category":"Sports","price":79.99,"stock":60}'
                          redis-cli SET product:19 '{"name":"Resistance Bands","category":"Sports","price":19.99,"stock":800}'
                          redis-cli SET product:20 '{"name":"Jump Rope","category":"Sports","price":14.99,"stock":650}'
                          redis-cli SET product:21 '{"name":"Coffee Maker","category":"Kitchen","price":89.99,"stock":110}'
                          redis-cli SET product:22 '{"name":"Blender","category":"Kitchen","price":59.99,"stock":95}'
                          redis-cli SET product:23 '{"name":"Toaster","category":"Kitchen","price":34.99,"stock":130}'
                          redis-cli SET product:24 '{"name":"Knife Set","category":"Kitchen","price":69.99,"stock":70}'
                          redis-cli SET product:25 '{"name":"Water Bottle","category":"Kitchen","price":12.99,"stock":1000}'
                          redis-cli LPUSH orders:user:1 1 3
                          redis-cli LPUSH orders:user:2 2
                          redis-cli LPUSH orders:user:3 5 11
                          redis-cli LPUSH orders:user:4 4
                          redis-cli LPUSH orders:user:5 2 3
                          redis-cli LPUSH orders:user:6 6 14
                          redis-cli LPUSH orders:user:7 7
                          redis-cli LPUSH orders:user:8 8 17
                          redis-cli LPUSH orders:user:9 11 12
                          redis-cli LPUSH orders:user:10 16 21
                          redis-cli LPUSH orders:user:11 17
                          redis-cli LPUSH orders:user:12 21
                          redis-cli LPUSH orders:user:13 22
                          redis-cli LPUSH orders:user:14 13
                          redis-cli LPUSH orders:user:15 9
                          redis-cli LPUSH orders:user:16 14
                          redis-cli LPUSH orders:user:17 18
                          redis-cli LPUSH orders:user:18 10
                          redis-cli LPUSH orders:user:19 15
                          redis-cli LPUSH orders:user:20 19
                          redis-cli LPUSH orders:user:21 23
                          redis-cli LPUSH orders:user:22 20
                          redis-cli LPUSH orders:user:23 24
                          redis-cli LPUSH orders:user:24 25
                          redis-cli LPUSH orders:user:25 1
                          echo "[seed] Redis: users, products, order lists"
                        fi

                        # === MongoDB ===
                        if mongo --quiet --eval "db.runCommand({ping:1})" > /dev/null 2>&1; then
                          mongo --quiet testdb --eval '
                            db.users.deleteMany({});
                            db.products.deleteMany({});
                            db.orders.deleteMany({});
                            db.users.insertMany([
                              { name: "Alice Johnson",   email: "alice@example.com",   age: 30 },
                              { name: "Bob Smith",       email: "bob@example.com",     age: 25 },
                              { name: "Charlie Brown",   email: "charlie@example.com", age: 35 },
                              { name: "Diana Prince",    email: "diana@example.com",   age: 28 },
                              { name: "Eve Adams",       email: "eve@example.com",     age: 22 },
                              { name: "Frank Miller",    email: "frank@example.com",   age: 40 },
                              { name: "Grace Lee",       email: "grace@example.com",   age: 33 },
                              { name: "Henry Wilson",    email: "henry@example.com",   age: 27 },
                              { name: "Irene Davis",     email: "irene@example.com",   age: 31 },
                              { name: "Jack Taylor",     email: "jack@example.com",    age: 29 },
                              { name: "Karen White",     email: "karen@example.com",   age: 36 },
                              { name: "Leo Garcia",      email: "leo@example.com",     age: 24 },
                              { name: "Mia Martinez",    email: "mia@example.com",     age: 26 },
                              { name: "Nathan Clark",    email: "nathan@example.com",  age: 38 },
                              { name: "Olivia Lewis",    email: "olivia@example.com",  age: 23 },
                              { name: "Paul Walker",     email: "paul@example.com",    age: 34 },
                              { name: "Quinn Hall",      email: "quinn@example.com",   age: 21 },
                              { name: "Rachel Young",    email: "rachel@example.com",  age: 32 },
                              { name: "Sam King",        email: "sam@example.com",     age: 37 },
                              { name: "Tina Wright",     email: "tina@example.com",    age: 28 },
                              { name: "Uma Scott",       email: "uma@example.com",     age: 26 },
                              { name: "Victor Green",    email: "victor@example.com",  age: 41 },
                              { name: "Wendy Baker",     email: "wendy@example.com",   age: 30 },
                              { name: "Xavier Adams",    email: "xavier@example.com",  age: 35 },
                              { name: "Yara Collins",    email: "yara@example.com",    age: 29 }
                            ]);
                            db.products.insertMany([
                              { name: "Laptop",                     category: "Electronics", price: 999.99, stock: 50 },
                              { name: "Wireless Mouse",             category: "Electronics", price:  29.99, stock: 200 },
                              { name: "Mechanical Keyboard",        category: "Electronics", price:  79.99, stock: 150 },
                              { name: "Monitor 27 inch",            category: "Electronics", price: 399.99, stock: 30 },
                              { name: "Noise-Cancelling Headphones", category: "Electronics", price: 149.99, stock: 75 },
                              { name: "Office Chair",               category: "Furniture",   price: 249.99, stock: 40 },
                              { name: "Standing Desk",              category: "Furniture",   price: 499.99, stock: 20 },
                              { name: "Desk Lamp",                  category: "Furniture",   price:  39.99, stock: 120 },
                              { name: "Bookshelf",                  category: "Furniture",   price: 129.99, stock: 55 },
                              { name: "Filing Cabinet",             category: "Furniture",   price:  89.99, stock: 65 },
                              { name: "Python Cookbook",            category: "Books",       price:  44.99, stock: 300 },
                              { name: "Design Patterns",            category: "Books",       price:  54.99, stock: 250 },
                              { name: "Clean Code",                 category: "Books",       price:  39.99, stock: 400 },
                              { name: "The Pragmatic Programmer",   category: "Books",       price:  49.99, stock: 180 },
                              { name: "Refactoring",                category: "Books",       price:  42.99, stock: 220 },
                              { name: "Running Shoes",              category: "Sports",      price: 119.99, stock: 90 },
                              { name: "Yoga Mat",                   category: "Sports",      price:  24.99, stock: 500 },
                              { name: "Dumbbell Set",               category: "Sports",      price:  79.99, stock: 60 },
                              { name: "Resistance Bands",           category: "Sports",      price:  19.99, stock: 800 },
                              { name: "Jump Rope",                  category: "Sports",      price:  14.99, stock: 650 },
                              { name: "Coffee Maker",               category: "Kitchen",     price:  89.99, stock: 110 },
                              { name: "Blender",                    category: "Kitchen",     price:  59.99, stock: 95 },
                              { name: "Toaster",                    category: "Kitchen",     price:  34.99, stock: 130 },
                              { name: "Knife Set",                  category: "Kitchen",     price:  69.99, stock: 70 },
                              { name: "Water Bottle",               category: "Kitchen",     price:  12.99, stock: 1000 }
                            ]);
                            db.orders.insertMany([
                              { userId: 1,  productId: 1,  quantity: 1, totalPrice: 999.99 },
                              { userId: 1,  productId: 3,  quantity: 1, totalPrice: 79.99 },
                              { userId: 2,  productId: 2,  quantity: 2, totalPrice: 59.98 },
                              { userId: 3,  productId: 5,  quantity: 1, totalPrice: 149.99 },
                              { userId: 4,  productId: 4,  quantity: 1, totalPrice: 399.99 },
                              { userId: 5,  productId: 2,  quantity: 3, totalPrice: 89.97 },
                              { userId: 5,  productId: 3,  quantity: 1, totalPrice: 79.99 },
                              { userId: 6,  productId: 6,  quantity: 1, totalPrice: 249.99 },
                              { userId: 7,  productId: 7,  quantity: 1, totalPrice: 499.99 },
                              { userId: 8,  productId: 8,  quantity: 2, totalPrice: 79.98 },
                              { userId: 9,  productId: 11, quantity: 1, totalPrice: 44.99 },
                              { userId: 9,  productId: 12, quantity: 1, totalPrice: 54.99 },
                              { userId: 10, productId: 16, quantity: 1, totalPrice: 119.99 },
                              { userId: 11, productId: 17, quantity: 3, totalPrice: 74.97 },
                              { userId: 12, productId: 21, quantity: 1, totalPrice: 89.99 },
                              { userId: 13, productId: 22, quantity: 1, totalPrice: 59.99 },
                              { userId: 14, productId: 13, quantity: 2, totalPrice: 79.98 },
                              { userId: 15, productId: 9,  quantity: 1, totalPrice: 129.99 },
                              { userId: 16, productId: 14, quantity: 1, totalPrice: 49.99 },
                              { userId: 17, productId: 18, quantity: 2, totalPrice: 39.98 },
                              { userId: 18, productId: 10, quantity: 1, totalPrice: 89.99 },
                              { userId: 19, productId: 15, quantity: 1, totalPrice: 42.99 },
                              { userId: 20, productId: 19, quantity: 4, totalPrice: 79.96 },
                              { userId: 21, productId: 23, quantity: 1, totalPrice: 34.99 },
                              { userId: 22, productId: 20, quantity: 2, totalPrice: 29.98 },
                              { userId: 23, productId: 24, quantity: 1, totalPrice: 69.99 },
                              { userId: 24, productId: 25, quantity: 5, totalPrice: 64.95 },
                              { userId: 25, productId: 1,  quantity: 1, totalPrice: 999.99 },
                              { userId: 1,  productId: 5,  quantity: 2, totalPrice: 299.98 },
                              { userId: 3,  productId: 11, quantity: 1, totalPrice: 44.99 },
                              { userId: 6,  productId: 14, quantity: 1, totalPrice: 49.99 },
                              { userId: 8,  productId: 17, quantity: 1, totalPrice: 24.99 },
                              { userId: 10, productId: 21, quantity: 1, totalPrice: 89.99 }
                            ]);
                          ' > /dev/null
                          echo "[seed] MongoDB: users, products, orders"
                        fi

                        # === PostgreSQL ===
                        if pg_isready -h /tmp -p 5432 > /dev/null 2>&1; then
                          psql -h /tmp -d testdb -c "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, age INTEGER);" 2>/dev/null
                          psql -h /tmp -d testdb -c "CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, category VARCHAR(50) NOT NULL, price NUMERIC(10,2) NOT NULL, stock INTEGER DEFAULT 0);" 2>/dev/null
                          psql -h /tmp -d testdb -c "CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id), product_id INTEGER REFERENCES products(id), quantity INTEGER NOT NULL DEFAULT 1, total_price NUMERIC(10,2), created_at TIMESTAMP DEFAULT NOW());" 2>/dev/null
                          psql -h /tmp -d testdb -c "INSERT INTO users (name, email, age) VALUES ('Alice Johnson','alice@example.com',30), ('Bob Smith','bob@example.com',25), ('Charlie Brown','charlie@example.com',35), ('Diana Prince','diana@example.com',28), ('Eve Adams','eve@example.com',22), ('Frank Miller','frank@example.com',40), ('Grace Lee','grace@example.com',33), ('Henry Wilson','henry@example.com',27), ('Irene Davis','irene@example.com',31), ('Jack Taylor','jack@example.com',29), ('Karen White','karen@example.com',36), ('Leo Garcia','leo@example.com',24), ('Mia Martinez','mia@example.com',26), ('Nathan Clark','nathan@example.com',38), ('Olivia Lewis','olivia@example.com',23), ('Paul Walker','paul@example.com',34), ('Quinn Hall','quinn@example.com',21), ('Rachel Young','rachel@example.com',32), ('Sam King','sam@example.com',37), ('Tina Wright','tina@example.com',28), ('Uma Scott','uma@example.com',26), ('Victor Green','victor@example.com',41), ('Wendy Baker','wendy@example.com',30), ('Xavier Adams','xavier@example.com',35), ('Yara Collins','yara@example.com',29) ON CONFLICT (email) DO NOTHING;" 2>/dev/null
                          psql -h /tmp -d testdb -c "INSERT INTO products (name, category, price, stock) VALUES ('Laptop','Electronics',999.99,50), ('Wireless Mouse','Electronics',29.99,200), ('Mechanical Keyboard','Electronics',79.99,150), ('Monitor 27 inch','Electronics',399.99,30), ('Noise-Cancelling Headphones','Electronics',149.99,75), ('Office Chair','Furniture',249.99,40), ('Standing Desk','Furniture',499.99,20), ('Desk Lamp','Furniture',39.99,120), ('Bookshelf','Furniture',129.99,55), ('Filing Cabinet','Furniture',89.99,65), ('Python Cookbook','Books',44.99,300), ('Design Patterns','Books',54.99,250), ('Clean Code','Books',39.99,400), ('The Pragmatic Programmer','Books',49.99,180), ('Refactoring','Books',42.99,220), ('Running Shoes','Sports',119.99,90), ('Yoga Mat','Sports',24.99,500), ('Dumbbell Set','Sports',79.99,60), ('Resistance Bands','Sports',19.99,800), ('Jump Rope','Sports',14.99,650), ('Coffee Maker','Kitchen',89.99,110), ('Blender','Kitchen',59.99,95), ('Toaster','Kitchen',34.99,130), ('Knife Set','Kitchen',69.99,70), ('Water Bottle','Kitchen',12.99,1000) ON CONFLICT DO NOTHING;" 2>/dev/null
                          psql -h /tmp -d testdb -c "INSERT INTO orders (user_id, product_id, quantity, total_price) VALUES (1,1,1,999.99), (1,3,1,79.99), (2,2,2,59.98), (3,5,1,149.99), (4,4,1,399.99), (5,2,3,89.97), (5,3,1,79.99), (6,6,1,249.99), (7,7,1,499.99), (8,8,2,79.98), (9,11,1,44.99), (9,12,1,54.99), (10,16,1,119.99), (11,17,3,74.97), (12,21,1,89.99), (13,22,1,59.99), (14,13,2,79.98), (15,9,1,129.99), (16,14,1,49.99), (17,18,2,39.98), (18,10,1,89.99), (19,15,1,42.99), (20,19,4,79.96), (21,23,1,34.99), (22,20,2,29.98), (23,24,1,69.99), (24,25,5,64.95), (25,1,1,999.99), (1,5,2,299.98), (3,11,1,44.99), (6,14,1,49.99), (8,17,1,24.99), (10,21,1,89.99) ON CONFLICT DO NOTHING;" 2>/dev/null
                          echo "[seed] PostgreSQL: users, products, orders"
                        fi

                        # === MySQL ===
                        if $MYSQL_BIN/mysqladmin ping -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" --silent 2>/dev/null; then
                          $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" testdb -e "CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, age INT);" 2>/dev/null
                          $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" testdb -e "CREATE TABLE IF NOT EXISTS products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, category VARCHAR(50) NOT NULL, price DECIMAL(10,2) NOT NULL, stock INT DEFAULT 0);" 2>/dev/null
                          $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" testdb -e "CREATE TABLE IF NOT EXISTS orders (id INT AUTO_INCREMENT PRIMARY KEY, user_id INT, product_id INT, quantity INT NOT NULL DEFAULT 1, total_price DECIMAL(10,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (user_id) REFERENCES users(id), FOREIGN KEY (product_id) REFERENCES products(id));" 2>/dev/null
                          $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" testdb -e "INSERT IGNORE INTO users (name, email, age) VALUES ('Alice Johnson','alice@example.com',30), ('Bob Smith','bob@example.com',25), ('Charlie Brown','charlie@example.com',35), ('Diana Prince','diana@example.com',28), ('Eve Adams','eve@example.com',22), ('Frank Miller','frank@example.com',40), ('Grace Lee','grace@example.com',33), ('Henry Wilson','henry@example.com',27), ('Irene Davis','irene@example.com',31), ('Jack Taylor','jack@example.com',29), ('Karen White','karen@example.com',36), ('Leo Garcia','leo@example.com',24), ('Mia Martinez','mia@example.com',26), ('Nathan Clark','nathan@example.com',38), ('Olivia Lewis','olivia@example.com',23), ('Paul Walker','paul@example.com',34), ('Quinn Hall','quinn@example.com',21), ('Rachel Young','rachel@example.com',32), ('Sam King','sam@example.com',37), ('Tina Wright','tina@example.com',28), ('Uma Scott','uma@example.com',26), ('Victor Green','victor@example.com',41), ('Wendy Baker','wendy@example.com',30), ('Xavier Adams','xavier@example.com',35), ('Yara Collins','yara@example.com',29);" 2>/dev/null
                          $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" testdb -e "INSERT IGNORE INTO products (name, category, price, stock) VALUES ('Laptop','Electronics',999.99,50), ('Wireless Mouse','Electronics',29.99,200), ('Mechanical Keyboard','Electronics',79.99,150), ('Monitor 27 inch','Electronics',399.99,30), ('Noise-Cancelling Headphones','Electronics',149.99,75), ('Office Chair','Furniture',249.99,40), ('Standing Desk','Furniture',499.99,20), ('Desk Lamp','Furniture',39.99,120), ('Bookshelf','Furniture',129.99,55), ('Filing Cabinet','Furniture',89.99,65), ('Python Cookbook','Books',44.99,300), ('Design Patterns','Books',54.99,250), ('Clean Code','Books',39.99,400), ('The Pragmatic Programmer','Books',49.99,180), ('Refactoring','Books',42.99,220), ('Running Shoes','Sports',119.99,90), ('Yoga Mat','Sports',24.99,500), ('Dumbbell Set','Sports',79.99,60), ('Resistance Bands','Sports',19.99,800), ('Jump Rope','Sports',14.99,650), ('Coffee Maker','Kitchen',89.99,110), ('Blender','Kitchen',59.99,95), ('Toaster','Kitchen',34.99,130), ('Knife Set','Kitchen',69.99,70), ('Water Bottle','Kitchen',12.99,1000);" 2>/dev/null
                          $MYSQL_BIN/mysql -u root --socket="$MYSQL_SOCK_DIR/$MYSQL_SOCK" testdb -e "INSERT IGNORE INTO orders (user_id, product_id, quantity, total_price) VALUES (1,1,1,999.99), (1,3,1,79.99), (2,2,2,59.98), (3,5,1,149.99), (4,4,1,399.99), (5,2,3,89.97), (5,3,1,79.99), (6,6,1,249.99), (7,7,1,499.99), (8,8,2,79.98), (9,11,1,44.99), (9,12,1,54.99), (10,16,1,119.99), (11,17,3,74.97), (12,21,1,89.99), (13,22,1,59.99), (14,13,2,79.98), (15,9,1,129.99), (16,14,1,49.99), (17,18,2,39.98), (18,10,1,89.99), (19,15,1,42.99), (20,19,4,79.96), (21,23,1,34.99), (22,20,2,29.98), (23,24,1,69.99), (24,25,5,64.95), (25,1,1,999.99), (1,5,2,299.98), (3,11,1,44.99), (6,14,1,49.99), (8,17,1,24.99), (10,21,1,89.99);" 2>/dev/null
                          echo "[seed] MySQL: users, products, orders"
                        fi

                        # === MariaDB ===
                        if $MARIA_BIN/mysqladmin ping -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" --silent 2>/dev/null; then
                          $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" testdb -e "CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, age INT);" 2>/dev/null
                          $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" testdb -e "CREATE TABLE IF NOT EXISTS products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, category VARCHAR(50) NOT NULL, price DECIMAL(10,2) NOT NULL, stock INT DEFAULT 0);" 2>/dev/null
                          $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" testdb -e "CREATE TABLE IF NOT EXISTS orders (id INT AUTO_INCREMENT PRIMARY KEY, user_id INT, product_id INT, quantity INT NOT NULL DEFAULT 1, total_price DECIMAL(10,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (user_id) REFERENCES users(id), FOREIGN KEY (product_id) REFERENCES products(id));" 2>/dev/null
                          $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" testdb -e "INSERT IGNORE INTO users (name, email, age) VALUES ('Alice Johnson','alice@example.com',30), ('Bob Smith','bob@example.com',25), ('Charlie Brown','charlie@example.com',35), ('Diana Prince','diana@example.com',28), ('Eve Adams','eve@example.com',22), ('Frank Miller','frank@example.com',40), ('Grace Lee','grace@example.com',33), ('Henry Wilson','henry@example.com',27), ('Irene Davis','irene@example.com',31), ('Jack Taylor','jack@example.com',29), ('Karen White','karen@example.com',36), ('Leo Garcia','leo@example.com',24), ('Mia Martinez','mia@example.com',26), ('Nathan Clark','nathan@example.com',38), ('Olivia Lewis','olivia@example.com',23), ('Paul Walker','paul@example.com',34), ('Quinn Hall','quinn@example.com',21), ('Rachel Young','rachel@example.com',32), ('Sam King','sam@example.com',37), ('Tina Wright','tina@example.com',28), ('Uma Scott','uma@example.com',26), ('Victor Green','victor@example.com',41), ('Wendy Baker','wendy@example.com',30), ('Xavier Adams','xavier@example.com',35), ('Yara Collins','yara@example.com',29);" 2>/dev/null
                          $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" testdb -e "INSERT IGNORE INTO products (name, category, price, stock) VALUES ('Laptop','Electronics',999.99,50), ('Wireless Mouse','Electronics',29.99,200), ('Mechanical Keyboard','Electronics',79.99,150), ('Monitor 27 inch','Electronics',399.99,30), ('Noise-Cancelling Headphones','Electronics',149.99,75), ('Office Chair','Furniture',249.99,40), ('Standing Desk','Furniture',499.99,20), ('Desk Lamp','Furniture',39.99,120), ('Bookshelf','Furniture',129.99,55), ('Filing Cabinet','Furniture',89.99,65), ('Python Cookbook','Books',44.99,300), ('Design Patterns','Books',54.99,250), ('Clean Code','Books',39.99,400), ('The Pragmatic Programmer','Books',49.99,180), ('Refactoring','Books',42.99,220), ('Running Shoes','Sports',119.99,90), ('Yoga Mat','Sports',24.99,500), ('Dumbbell Set','Sports',79.99,60), ('Resistance Bands','Sports',19.99,800), ('Jump Rope','Sports',14.99,650), ('Coffee Maker','Kitchen',89.99,110), ('Blender','Kitchen',59.99,95), ('Toaster','Kitchen',34.99,130), ('Knife Set','Kitchen',69.99,70), ('Water Bottle','Kitchen',12.99,1000);" 2>/dev/null
                          $MARIA_BIN/mariadb -u root --socket="$MARIA_SOCK_DIR/$MARIA_SOCK" testdb -e "INSERT IGNORE INTO orders (user_id, product_id, quantity, total_price) VALUES (1,1,1,999.99), (1,3,1,79.99), (2,2,2,59.98), (3,5,1,149.99), (4,4,1,399.99), (5,2,3,89.97), (5,3,1,79.99), (6,6,1,249.99), (7,7,1,499.99), (8,8,2,79.98), (9,11,1,44.99), (9,12,1,54.99), (10,16,1,119.99), (11,17,3,74.97), (12,21,1,89.99), (13,22,1,59.99), (14,13,2,79.98), (15,9,1,129.99), (16,14,1,49.99), (17,18,2,39.98), (18,10,1,89.99), (19,15,1,42.99), (20,19,4,79.96), (21,23,1,34.99), (22,20,2,29.98), (23,24,1,69.99), (24,25,5,64.95), (25,1,1,999.99), (1,5,2,299.98), (3,11,1,44.99), (6,14,1,49.99), (8,17,1,24.99), (10,21,1,89.99);" 2>/dev/null
                          echo "[seed] MariaDB: users, products, orders"
                        fi

                        # === CockroachDB ===
                        if cockroach sql --host=localhost --port=26257 --insecure -e "SELECT 1" > /dev/null 2>&1; then
                          cockroach sql --host=localhost --port=26257 --insecure --database=testdb -e "
                            CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, age INT);
                            CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, category VARCHAR(50) NOT NULL, price DECIMAL(10,2) NOT NULL, stock INT DEFAULT 0);
                            CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id), product_id INT REFERENCES products(id), quantity INT NOT NULL DEFAULT 1, total_price DECIMAL(10,2), created_at TIMESTAMP DEFAULT NOW());
                          " 2>/dev/null
                          cockroach sql --host=localhost --port=26257 --insecure --database=testdb -e "INSERT INTO users (name, email, age) VALUES ('Alice Johnson','alice@example.com',30), ('Bob Smith','bob@example.com',25), ('Charlie Brown','charlie@example.com',35), ('Diana Prince','diana@example.com',28), ('Eve Adams','eve@example.com',22), ('Frank Miller','frank@example.com',40), ('Grace Lee','grace@example.com',33), ('Henry Wilson','henry@example.com',27), ('Irene Davis','irene@example.com',31), ('Jack Taylor','jack@example.com',29), ('Karen White','karen@example.com',36), ('Leo Garcia','leo@example.com',24), ('Mia Martinez','mia@example.com',26), ('Nathan Clark','nathan@example.com',38), ('Olivia Lewis','olivia@example.com',23), ('Paul Walker','paul@example.com',34), ('Quinn Hall','quinn@example.com',21), ('Rachel Young','rachel@example.com',32), ('Sam King','sam@example.com',37), ('Tina Wright','tina@example.com',28), ('Uma Scott','uma@example.com',26), ('Victor Green','victor@example.com',41), ('Wendy Baker','wendy@example.com',30), ('Xavier Adams','xavier@example.com',35), ('Yara Collins','yara@example.com',29) ON CONFLICT (email) DO NOTHING;" 2>/dev/null
                          cockroach sql --host=localhost --port=26257 --insecure --database=testdb -e "INSERT INTO products (name, category, price, stock) VALUES ('Laptop','Electronics',999.99,50), ('Wireless Mouse','Electronics',29.99,200), ('Mechanical Keyboard','Electronics',79.99,150), ('Monitor 27 inch','Electronics',399.99,30), ('Noise-Cancelling Headphones','Electronics',149.99,75), ('Office Chair','Furniture',249.99,40), ('Standing Desk','Furniture',499.99,20), ('Desk Lamp','Furniture',39.99,120), ('Bookshelf','Furniture',129.99,55), ('Filing Cabinet','Furniture',89.99,65), ('Python Cookbook','Books',44.99,300), ('Design Patterns','Books',54.99,250), ('Clean Code','Books',39.99,400), ('The Pragmatic Programmer','Books',49.99,180), ('Refactoring','Books',42.99,220), ('Running Shoes','Sports',119.99,90), ('Yoga Mat','Sports',24.99,500), ('Dumbbell Set','Sports',79.99,60), ('Resistance Bands','Sports',19.99,800), ('Jump Rope','Sports',14.99,650), ('Coffee Maker','Kitchen',89.99,110), ('Blender','Kitchen',59.99,95), ('Toaster','Kitchen',34.99,130), ('Knife Set','Kitchen',69.99,70), ('Water Bottle','Kitchen',12.99,1000) ON CONFLICT DO NOTHING;" 2>/dev/null
                          cockroach sql --host=localhost --port=26257 --insecure --database=testdb -e "INSERT INTO orders (user_id, product_id, quantity, total_price) VALUES (1,1,1,999.99), (1,3,1,79.99), (2,2,2,59.98), (3,5,1,149.99), (4,4,1,399.99), (5,2,3,89.97), (5,3,1,79.99), (6,6,1,249.99), (7,7,1,499.99), (8,8,2,79.98), (9,11,1,44.99), (9,12,1,54.99), (10,16,1,119.99), (11,17,3,74.97), (12,21,1,89.99), (13,22,1,59.99), (14,13,2,79.98), (15,9,1,129.99), (16,14,1,49.99), (17,18,2,39.98), (18,10,1,89.99), (19,15,1,42.99), (20,19,4,79.96), (21,23,1,34.99), (22,20,2,29.98), (23,24,1,69.99), (24,25,5,64.95), (25,1,1,999.99), (1,5,2,299.98), (3,11,1,44.99), (6,14,1,49.99), (8,17,1,24.99), (10,21,1,89.99) ON CONFLICT DO NOTHING;" 2>/dev/null
                          echo "[seed] CockroachDB: users, products, orders"
                        fi

                        echo ""
                        echo "Ready. Run bridge migrations from the repo root:"
                        echo "  bridge migrate --source-provider postgres --source-url 'postgresql://127.0.0.1:5432/testdb?sslmode=disable' --dest-provider cockroachdb --dest-url 'cockroachdb://root@localhost:26257/testdb?sslmode=disable'"
                        echo ""
          '';
        };
      }
    );
}
