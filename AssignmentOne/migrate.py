import psycopg2
import neo4j

DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)

table_names = ["actor", "address", "category", "city", "country", "customer", "film", "film_actor", "film_category",
               "inventory", "language", "payment", "rental", "staff", "store"]

tables_with_values = {}

# part postgress_to from component diagram
try:
    conn = psycopg2.connect(host="localhost", port="10000", database="postgres", user="postgres", password="mew")
    cur = conn.cursor()

    for name in table_names:
        select = "SELECT * FROM "+name
        cur.execute(select)
        res = cur.fetchall()
        tables_with_values[name] = res

    cur.close()
finally:
    conn.close()

# connect to neo4j
# default password was neo4j
# was changed manually for "mew" in a browser interface
driver = None
# до 800 вставляет норм
# part neo4j_to from component diagram
try:
    driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "mew"))


    with driver.session() as sess:

        lang = list(map(list, tables_with_values['language']))
        sess.run("UNWIND $languages AS lan "
                 "CREATE (l:Language {language_id:lan[0], name:lan[1], last_update: lan[2]})",
                 parameters={'languages': lang})

        print("language done")

        # because neo4j plugin is in the development version, it is unstable,
        # hence, some creations of nodes are separated, otherwise, connection ruins
        # language_id is not saved as relation created
        films = list(map(list, tables_with_values['film']))
        films_chuncks = [films[:800], films[800:]]
        for fms in films_chuncks:
            sess.run("UNWIND $films AS film "
                     "MATCH (l:Language { language_id: film[4] })"
                     "MERGE(l)-[:FILM_LANGUAGE]->(f:Film {film_id: film[0], title: film[1], description: film[2], "
                     "release_year: film[3], rental_duration: film[5], rental_rate: film[6], length: film[7], "
                     "replacement_cost: film[8], rating: film[9], last_update: film[10],  spesific_features: film[11], "
                     "fulltext: film[12]})",
                     parameters={'films': fms})

        print("films done")

        cats = list(map(list, tables_with_values['category']))
        sess.run("UNWIND $cats AS ct "
                 "CREATE (cat:Category {category_id: ct[0], name:ct[1], last_update:ct[2]})",
                 parameters={'cats': cats})

        print("category done")

        film_cats = list(map(list, tables_with_values['film_category']))
        # film_category table is transformed to relation
        sess.run("UNWIND $films_VS_categories AS film_cat "
                 "MATCH (f:Film {film_id:film_cat[0]}), (cat:Category{category_id:film_cat[1]}) "
                 "MERGE (f)-[fc: FILM_CATEGORY {last_update: film_cat[2]}]-(cat)",
                 parameters={'films_VS_categories': film_cats})

        print("film_category done")

        act = list(map(list, tables_with_values['actor']))
        sess.run("UNWIND $actors AS actor "
                 "CREATE ( ac:Actor {actor_id: actor[0], first_name: actor[1], last_name: actor[2], "
                 "last_update: actor[3]})",
                 parameters={'actors': act})

        print("actor done")

        # film_actor table is transformed to relation
        film_actors = list(map(list, tables_with_values['film_actor']))
        sess.run("UNWIND $film_actor AS actfilm "
                 "MATCH (f:Film {film_id:actfilm[0]}), (ac:Actor{actor_id: actfilm[1]}) "
                 "MERGE (f)-[fa:FILM_ACTOR {last_update:actfilm[2]}]-(ac)",
                 parameters={'film_actor': film_actors})
        print("film_actor done")

        # here film_id is deleted as there is no need in it
        # as far as the realation between the corresponding film and enventory are establish right after creation of the inventory
        # the film_id was needed only for connecting this tables and because the nodes are already connected we can delete it
        # without loosing any info
        inventories = list(map(list, tables_with_values['inventory']))
        sess.run("UNWIND $inventory AS inv "
                 "MATCH (f:Film {film_id:inv[1]}) "
                 "MERGE (f)-[fi:INVENTORY_FILM]->"
                 "(invent: Inventory {inventory_id:inv[0], store_id: inv[2], last_update: inv[3]})",
                 parameters={'inventory': inventories})

        print("inventory done")

        countries = list(map(list, tables_with_values['country']))
        sess.run("UNWIND $country as c "
                 "CREATE (count: COUNTRY {country_id:c[0], country:c[1], last_update:c[2]})",
                 parameters={'country': countries})

        print("country done")

        city = list(map(list, tables_with_values['city']))
        sess.run("UNWIND $cities as c "
                 "MATCH (count: COUNTRY {country_id : c[2]}) "
                 "MERGE (count)-[chc : COUNTRY_HAS_CITY]->(city :City {city_id: c[0], city: c[1], last_update: c[3]})",
                 parameters={'cities': city})

        print("city done")

        #  with because of nulls in address - merge fails with nulls
        addrs = list(map(list, tables_with_values['address']))
        sess.run("UNWIND $add as adrs "
                 "CREATE (addr: ADDRESS {address_id: adrs[0], address: adrs[1], address2: adrs[2], district : adrs[3],"
                 "postal_code: adrs[5], phone:adrs[6], last_apdate: adrs[7]}) "
                 "WITH addr , adrs[4] as id_to_city "
                 "MATCH (city :City {city_id : id_to_city }) "
                 "MERGE (city)-[cha: CITY_HAS_ADDRESS]-(addr)",
                 parameters={'add': addrs})

        print("address done")

        customers = list(map(list, tables_with_values['customer']))
        sess.run("UNWIND $customer as c "
                 "MATCH (addr: ADDRESS { address_id : c[5]}) "
                 "MERGE (addr)-[custadr: CUSTOMER_HAS_ADDRESS]->(cust : Customer {customer_id : c[0], store_id : c[1], "
                 "first_name : c[2], last_name : c[3], email : c[4], activebool: c[6], create_date : c[7],"
                 "last_update : c[8], active : c[9]})",
                 parameters={'customer': customers})

        print("customer done")

        # address_id, store_id are not stored
        # with because of nulls in picture
        staffs = list(map(list, tables_with_values['staff']))
        for i in range(len(staffs)):
            if staffs[i][10] is None:
                pass
            else:
                staffs[i][10] = bytes(staffs[i][10])
        sess.run("UNWIND $staff as s "
                 "CREATE (stf : Staff {staff_id: s[0], first_name: s[1], last_name: s[2], email: s[4], active: s[6], "
                 "username: s[7], password: s[8], last_update: s[9], picture: s[10]}) "
                 "WITH stf , s[3] as id_for_address, s[5] as id_for_store "
                 "MATCH (addr: ADDRESS {address_id : id_for_address }), (st: Store {store_id : id_for_store}) "
                 "MERGE (stf)-[stfadrs : STAFF_ADDRESS]-(addr) ",
                 parameters={'staff': staffs})

        print("staff done")

        stores = list(map(list, tables_with_values['store']))
        sess.run("UNWIND $store as s "
                 "MATCH (addr: ADDRESS {address_id : s[2]}), (stf:Staff {staff_id: s[1] }) "
                 "MERGE (addr)-[storeadr: STORE_ADDRESS]->"
                 "(st :Store {store_id: s[0], last_update: s[3]}) "
                 "MERGE (st)-[stfstore: MANAGER_STAFF]-(stf)",
                 parameters={'store': stores})

        print("store done")

        # same as previous, we do not save inventory_id
        # create with because of null properties
        rentals = list(map(list, tables_with_values['rental']))
        rentals_chuncks = [rentals[:7000], rentals[7000:14000], rentals[14000:]]
        for i in range(len(rentals_chuncks)):
            sess.run("UNWIND $rental as rent "
                     "CREATE (r: Rental {rental_id:rent[0], rental_date:rent[1], return_rate:rent[4], last_update: rent[6]}) "
                     "WITH r,  rent[2] as inv_id, rent[3] as cust_id, rent[5] as stf_id "
                     "MATCH (invent: Inventory {inventory_id : inv_id}), (cust : Customer {customer_id : cust_id}), "
                     "(stf : Staff {staff_id: stf_id }) "
                     "MERGE (invent)-[ri: RENTAL_PROVIDES_INVENTORY ]-(r) "
                     "MERGE (r)-[rc: CUSTOMER_RENTS]-(cust) "
                     "MERGE (r)-[rstf :RENTAL_STAFF]-(stf)",
                     parameters={'rental': rentals_chuncks[i]})

        print("rental done")

        # customer_id, address_id is not saved
        pays = list(map(list, tables_with_values['payment']))
        pays_chuncks = [pays[:5000], pays[5000:10000], pays[10000:]]
        for cur_pay in pays_chuncks:
            sess.run("UNWIND $payment as p "
                     "MATCH (cust : Customer { customer_id : p[1] }), (r: Rental {rental_id : p[3]}),"
                     "(stf: Staff {staff_id : p[2]}) "
                     "MERGE (cust)-[pc : PAYMENT_CUSTOMER]->(pay: Payment {payment_id:p[0], amount:p[4], payment_date:p[5]})"
                     " MERGE (stf)-[pstf : PAYMENT_STAFF]-> (pay) "
                     "MERGE (r)-[pr : PAYMENT_RENTAL]->(pay)",
                     parameters={'payment': cur_pay})

        print("payment done")


finally:
    if driver is not None:
        driver.close()

