# static vars and static method
# to count no of objects created for a class

class Sample:
    # class variable/ stativ var
    x = 0
    def __init__(self):
        Sample.x +=1

    @staticmethod
    def display():
        print('no of object created: ', Sample.x)


s1 = Sample()
print(s1.x)
s1.display()

s2 = Sample()
print(s2.x)
s2.display()

s3 = Sample()
print(s3.x)
s3.display()
