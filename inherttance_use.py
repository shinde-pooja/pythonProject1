# use.py to use Student class

from prg.Student_inher import *
st = Student()
st.setId(10)
st.setName('shrinu')
st.setAddress('auranbagad')
st.setMarks(980)

print('id:', st.getId())
print('name:',st.getName())
print('address:',st.getAddress())
print('marks:',st.getMarks())
