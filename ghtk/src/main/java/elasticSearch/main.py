import json
import numpy
import pandas as pd
from pyvi import ViTokenizer
from sklearn.feature_extraction.text import TfidfVectorizer

if __name__ == '__main__':
    vectorizer = TfidfVectorizer()
    data=""
    fRead = open("C:\\Users\\minhs\\IdeaProjects\\ghtk\\src\\main\\data\\data_1.json", "r", encoding="utf8")
    fWrite = open("C:\\Users\\minhs\\IdeaProjects\\ghtk\\src\\main\\data\\minh_1.json", "a", encoding="utf8")
    Lines = fRead.readlines()
    count = 0
    f = lambda x: x.replace("_", " ")
    # Strips the newline character
    for line in Lines:
        count += 1
        #print("Line{}: {}".format(count, line.strip()))
        if count % 2 == 0:

            json_object = json.loads(line.strip())
            xyz = ViTokenizer.tokenize(json_object["title"])

            data+=xyz+" "
            

    data=[data.lower()]
    X = vectorizer.fit_transform(data)
    a= list(map(f,vectorizer.get_feature_names()))
    count=0
    
    for line in Lines:
        count += 1
        #print("Line{}: {}".format(count, line.strip()))
        if count % 2 != 0:
            fWrite.write(line)
        else:
            json_object = json.loads(line.strip())
            #print(json_object["title"])
            xyz = list(map(f,ViTokenizer.tokenize(json_object["title"]).split()))
            #xyz = list(filter(lambda x:(" " in x),xyz))
            
            chuoi="{" + "\"suggest_title\": ["
            for i in xyz:
                if(i.lower() in a):
                    chuoi+='{"input" : "'+i +'" ,' +" \"" +"weight"+ '" : '+str(round(X[0,a.index(i.lower())]*10000)) + "}"+"," 
            chuoi=chuoi[:-1]    
            chuoi+="]}\n"
            fWrite.write(chuoi)

            

            #print(list(map(f, xyz)))
#            fWrite.write("{" + "\"suggest_title\": [\"" + '", "'.join(list(map(f, xyz))) + "\"]}\n")
    #print(data)

    fRead.close()
    fWrite.close()
    