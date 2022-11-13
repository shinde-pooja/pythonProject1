# static / class var and class method

class Sample:

    # below is class var
    x = 10

    # below is class method
    @classmethod
    def modify(cls):
        cls.x +=1

s1 = Sample()
s2= Sample()
print(s1.x,s2.x)

s1.modify()
print(s1.x, s2.x)

s1.x+=1
print(s1.x,s2.x)

