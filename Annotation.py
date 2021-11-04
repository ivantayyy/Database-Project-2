import psycopg2
import re


def getQEP(user, pw):
    # Connect to your postgres DB
    conn = psycopg2.connect(host="localhost", port=5432, database="TPC-H", user=user, password=pw)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a query
    sql_query = "SELECT * from customer C, orders O where C.c_custkey = O.o_custkey"
    sql_query = "SELECT PS.ps_availqty from partsupp PS, supplier S, customer C, orders O where S.s_suppkey = PS.ps_suppkey and C.c_custkey = O.o_custkey ORDER BY S.s_name"
    cur.execute(cur.mogrify('explain analyze ' + sql_query))

    # Retrieve query results
    query_results = cur.fetchall()
    print(query_results)

    cur.close()
    conn.close()


def preprocessing(qep):
    count = 0;
    filteredSteps=[]
    for step in qep:
        plan = step[0]
        # remove unnecessary brackets
        # print("count= "+str(count))
        # print(plan)
        if '->' in plan:
            plan = re.sub("[(\[].*?[\)\]]", "", plan)
            plan = plan.replace("->","")
            layer = len(plan) - len(plan.lstrip(' '))
            strLayer = str(layer)
            if layer >= 0 and layer <=9:
                strLayer = "0"+strLayer
            plan = strLayer+ " "+ plan.strip()

            filteredSteps.append(plan)
            count += 1
        else:
            if count>0 and "Planning" not in plan and "Execution" not in plan:
                filteredSteps[count - 1] += " with "+plan.strip()
            else:
                plan = re.sub("[(\[].*?[\)\]]", "", plan)
                layer = len(plan) - len(plan.lstrip(' '))
                strLayer = str(layer)
                if layer >= 0 and layer <= 9:
                    strLayer = "0" + strLayer
                plan = strLayer + " " + plan.strip()

                filteredSteps.append(plan)
                count += 1


    print('steps')
    for step in filteredSteps:
        print(step)
    print()

    return filteredSteps

def annotate(querySteps):
    length = len(querySteps)
    holdOutput = None
    output = None
    useHold = False
    while length >=1:
        curStep = querySteps[length-1]
        curLayer = int(querySteps[length-1][0:2])
        aboveLayer = int(querySteps[length - 2][0:2])
        #print("length: "+ str(length)+" querystep: "+querySteps[length-1])


        if "Join" in curStep or "Loop" in curStep: #for operations that need 2 inputs
            if useHold:
                print(curStep[2:] + " on " + holdOutput[2:] + " and on " + output[2:])
                useHold=False
            else:
                print(curStep[2:] + " on " + querySteps[length+1][2:] + " and on " + output[2:])
                if holdOutput is not None:
                    #print(holdOutput[:2] + " " + curStep)
                    if int(holdOutput[:2]) == int(curStep[:2]):
                        #print("usehold is true")
                        useHold = True
            output = curStep
        else: #for operations that need 1 or 0 inputs
            if holdOutput is not None:
                #print(holdOutput[:2] + " " + curStep)
                if int(holdOutput[:2]) == int(curStep[:2]):
                    useHold = True
            if curLayer<aboveLayer: # check if need to store output for far future use
                print(curStep[2:])
                holdOutput = curStep
            else: # output not needed in far future
                if "Hash" in curStep: # for operation that takes 1 input
                    if output is not None:
                        print(curStep[2:]+" on "+ output[2:])
                        output = curStep
                    else: # for operation that takes 0 input
                        print(curStep[2:])
                        output = curStep
                else:
                    print(curStep[2:])
                    output = curStep



        length-=1


def main():
    # getQEP("postgres", "123")
    #qep = [('Hash Join  (cost=10330.00..108538.61 rows=1500000 width=266) (actual time=633.256..6622.343 rows=1500000 loops=1)',), ('  Hash Cond: (o.o_custkey = c.c_custkey)',), ('  ->  Seq Scan on orders o  (cost=0.00..41095.00 rows=1500000 width=107) (actual time=1.757..1270.994 rows=1500000 loops=1)',), ('  ->  Hash  (cost=5085.00..5085.00 rows=150000 width=159) (actual time=625.174..625.175 rows=150000 loops=1)',), ('        Buckets: 32768  Batches: 8  Memory Usage: 3858kB',), ('        ->  Seq Scan on customer c  (cost=0.00..5085.00 rows=150000 width=159) (actual time=0.849..300.105 rows=150000 loops=1)',), ('Planning Time: 25.436 ms',), ('Execution Time: 6715.488 ms',)]

    qep = [('Gather Merge  (cost=174487386432.28..291162198935.89 rows=1000000000000 width=30)',), ('  Workers Planned: 2',), ('  ->  Sort  (cost=174487385432.26..175737385432.26 rows=500000000000 width=30)',), ('        Sort Key: s.s_name',), ('        ->  Parallel Hash Join  (cost=2817785304.56..5552198835.64 rows=500000000000 width=30)',), ('              Hash Cond: (o.o_custkey = c.c_custkey)',), ('              ->  Parallel Seq Scan on orders o  (cost=0.00..32345.00 rows=625000 width=4)',), ('              ->  Parallel Hash  (cost=1802160304.56..1802160304.56 rows=50000000000 width=34)',), ('                    ->  Nested Loop  (cost=447.42..1802160304.56 rows=50000000000 width=34)',), ('                          ->  Hash Join  (cost=447.00..22106.70 rows=333333 width=30)',), ('                                Hash Cond: (ps.ps_suppkey = s.s_suppkey)',), ('                                ->  Parallel Seq Scan on partsupp ps  (cost=0.00..20784.33 rows=333333 width=8)',), ('                                ->  Hash  (cost=322.00..322.00 rows=10000 width=30)',), ('                                      ->  Seq Scan on supplier s  (cost=0.00..322.00 rows=10000 width=30)',), ('                          ->  Index Only Scan using customer_pkey on customer c  (cost=0.42..3906.42 rows=150000 width=4)',)]
    querySteps = preprocessing(qep)
    annotate(querySteps)


if __name__ == '__main__':
    main()
