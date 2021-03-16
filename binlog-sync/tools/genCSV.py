import csv
def to_str(x):
    s=""
    while x !=0:
        s+=chr(ord('a')+x%26)
        x//=26
    return s
def create_csv():
    path="aa.csv"
    with open("../data/log.csv","w") as f:
        csv_write=csv.writer(f)
        for i in range(1,200):
            val=[i,i,to_str(i),to_str(i),'I']
            csv_write.writerow(val)
if __name__ == "__main__":
    create_csv()
