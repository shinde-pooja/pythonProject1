# static method to find square root value

class Myclass:

    @staticmethod
    def sroot(x):
        res = x ** 0.5
        return res

x = int(input('enter no: '))
res = Myclass.sroot(x)
print('square root of',x,'is',res)

m = Myclass()
res = m.sroot(16)
print(res)