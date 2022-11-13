# going into future or past

from datetime import *
dt = datetime(2022,6,16,10,4,30)
duration = timedelta(weeks=3,days=10,hours=10,minutes=50)
print('future date and time= ',dt+duration)
print('past date and time = ', dt-duration)
