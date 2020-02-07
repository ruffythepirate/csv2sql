FROM lolhens/ammonite:latest


WORKDIR "/usr/app"
COPY csv2sql.sc .


RUN ./csv2sql.sc; exit 0 

ENTRYPOINT ["/usr/app/csv2sql.sc"]
