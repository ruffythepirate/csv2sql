FROM ruffythepirate/ammonite:latest

WORKDIR "/usr/app"
COPY csv2sql.sc .

ENTRYPOINT ["/usr/local/bin/amm", "csv2sql.sc"]
