 # python file is needed to represent the results in form of table

import neo4j
import csv

q2 = \
   "CALL {" \
   "MATCH (a1:Actor)-[:FILM_ACTOR]-(f:Film)-[:FILM_ACTOR]-(a2:Actor) " \
   "RETURN a1.actor_id as aOne, a2.actor_id as aTwo, count(f) as FILMS " \
   "UNION " \
   "MATCH (a3:Actor)-[:FILM_ACTOR]-(f:Film)" \
   "RETURN a3.actor_id as aOne,a3.actor_id as aTwo, count(f) as FILMS} " \
   "WITH aOne, aTwo, FILMS ORDER BY aOne, aTwo " \
   "RETURN collect(aOne), collect(aTwo), collect(FILMS)"

path = "/Users/polinaturiseva/Desktop/dmd2Ass1ToSubmit/query2-report.csv"

def csv_writer(data):
    """
    Write data to a CSV file path
    """
    with open(path, "w", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for line in data:

            writer.writerow(line.split(","))


driver = None
try:
    driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "mew"))
    with driver.session() as sess:
        res    = sess.run(q2)
        res    = res.values()[0]
        actor1 = res[0]
        actor2 = res[1]
        films  = res[2]
        num_of_actors = actor1[-1]
        a1    = 0
        a2    = 0
        f     =0
        lines = []
        line_1 = "A,"
        for i in range(1, num_of_actors+1):
            line_1 = line_1+f'{i},'
            cur_line = f"{i},"
            for j in range(1, num_of_actors + 1):
                if actor2[a2]!=j:
                    cur_line = cur_line+'0'
                else:
                    cur_line = cur_line + f'{films[f]}'
                    f  += 1
                    a2 += 1
                if j!=200:
                    cur_line = cur_line +','
            lines.append(cur_line)

        line_1 = line_1[:-1]
        lines.insert(0, line_1)
        csv_writer(lines)
finally:
    if driver is not None:
        driver.close()