set -eux

go build -o bin/echo -C echo
(cd maelstrom && ./maelstrom test -w echo --bin ../echo/bin/echo --node-count 1 --time-limit 10)


go build -o bin/uniqueid -C uniqueid
(cd maelstrom && ./maelstrom test -w unique-ids --bin ../uniqueid/bin/uniqueid --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition)

go build -o bin/broadcast -C broadcast
(cd maelstrom && ./maelstrom test -w broadcast --bin ../broadcast/bin/broadcast --node-count 10 --time-limit 20 --rate 10 --nemesis partition)

go build -o bin/gocounter -C gocounter
(cd maelstrom && ./maelstrom test -w g-counter --bin ../gocounter/bin/gocounter --node-count 10 --rate 100 --time-limit 20 --nemesis partition)

go build -o bin/kafka -C kafka
(cd maelstrom && ./maelstrom test -w kafka --bin ../kafka/bin/kafka --node-count 5 --concurrency 2n --time-limit 20 --rate 1000)
