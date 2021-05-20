from neo4j import GraphDatabase

class GraphDB:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)
    
    def get_movies_by_producer(self,prod_name):
        with self.driver.session() as session:
            result = session.run("MATCH (mv:Movie)-[:DIRECTED]-(p:Person {name: '%s'}) return mv;"%prod_name)
            for i in result:
                print(i['mv']['title'])
            
    def get_people_by_colleague(self,coll_name):
        with self.driver.session() as session:
            result = session.run("MATCH (p:Person{name: '%s'})-[*]->(:Movie)<-[*]-(cp:Person) WHERE p <> cp RETURN DISTINCT cp;"%coll_name)
            for i in result:
                print(i['cp']['name'])

    def get_count_actors_of_every_movie(self):
        with self.driver.session() as session:
            result =  session.run("MATCH (pers:Person)-[:ACTED_IN]->(m:Movie) RETURN DISTINCT m.title, COUNT(pers.name);")
           
            for i in result:
                print(i['m.title']+' '+str(i['COUNT(pers.name)']))

      
if __name__ == "__main__":
    mv_graph = GraphDB("bolt://localhost:7687", "neo4j", "password")
    foo = 'test'
    while True:
        cmd = input('Введите команду или help-> ')
        
        if (cmd.split(' ')[0] == 'get_movie'):
            prod_name =  cmd.split(' ')[1]
            for i in cmd.split(' ')[2:]:
                prod_name = prod_name + ' ' + i 
            mv_graph.get_movies_by_producer(prod_name)  

        elif (cmd.split(' ')[0] == 'get_people'):
            coll_name =  cmd.split(' ')[1]
            for i in cmd.split(' ')[2:]:
                coll_name = coll_name + ' ' + i 
            mv_graph.get_people_by_colleague(coll_name)  


        elif (cmd.split(' ')[0] == 'count_actors'):
            mv_graph.get_count_actors_of_every_movie()    

        elif (cmd.split(' ')[0] == 'help'):
            print("""
            get_movie <Полное имя режиссёра> -найдёт все фильмы заданного режиссёра
            get_people <Полное имя человека> -найдёт всех коллег заданного человека
            count_actors - даст список фильмов и количество актёров в них
            exit - выход из приложения
            help - вывод этого сообщения
            """)

        elif (cmd.split(' ')[0] == 'exit'):
            print('До свидания!')
            break


        else:
            print('Неверная команда! Введите help для получения списка команд\n')




    mv_graph.close()
