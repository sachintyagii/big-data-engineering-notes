from pyspark import SparkContext

def add_col(data):
    data_split = data.split(",")
    age = int(data_split[1])

    result = 'N'
    if age > 18:
        result = 'Y'
    
    data_split.append(result)

    return ",".join(data_split)

if __name__ == "__main__":
    sc = SparkContext("local[*]", "age18")
    user_data = sc.textFile("E:\\dataset\\users.dataset1")
    user_data_add_col = user_data.map(add_col)

    result = user_data_add_col.collect()

    for i in result:
        print(f'{i} -> {type(i)}')
