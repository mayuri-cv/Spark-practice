d = {}
with open('C:/Users/Mayuri Dangare/PycharmProjects/Spark-practice/resource/x.txt', 'r') as fp:
    for line in fp:
        line = line.strip()
        words = line.split(' ')
        for word in words:
            if word in d:
                d[word] += 1

            else:
                d[word] = 1
    print(d)
