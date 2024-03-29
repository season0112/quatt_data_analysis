from redis import Redis
from enum import IntFlag

class datatype(IntFlag):
    double = 1
    uint = 2
    bool = 3


class QuattRedis(Redis):
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        super().__init__(host=host, port=port, db=db, decode_responses=True, password=None)
    
    def qget(self, address, dtype: datatype = datatype.double):
        value = self.get(address)
        if not value:
            return value
        if dtype == datatype.double:
            try:
                value =  float(value)
            except ValueError as err:
                raise ValueError(f"Error while casting {value} in redis address {address} to float. \n{err}")
        elif dtype == datatype.uint:
            try:
                value =  int(value)
            except ValueError as err:
                raise ValueError(f"Error while casting {value} in redis address {address} to int. \n{err}")
        elif dtype == datatype.bool:
            if value == "0" or value == "":
                value = False
            else:
                value = True

        return value
    
    def to_dict(self):
        keys = self.keys("*")
        keys.sort()
        values = self.mget(keys)
        return dict(zip(keys, values))
        

    
if __name__ == '__main__':
    from redis.exceptions import ConnectionError as RedisConnectionError
    r = QuattRedis()
    try:
        print(r.ping())
    except:
        print(False)
    # r_dict = {
    #     1: "John",
    #     2: "Doe"
    # }
    # r.mset(r_dict)
    # # r.set('foo', 'bar')
    # keys = r.keys("*")
    # for key in keys:
    #     print(f"{key}: {r.get(key)}")

    # print(r.items())