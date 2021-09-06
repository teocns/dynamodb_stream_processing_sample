class GlobalCache:
    """
    A singleton static global cache for storing data.
    get and set methods must be static
    """
    _cache = {}

    @staticmethod
    def get(key):
        """
        Return the value of the key from the cache.
        If the key does not exist, return None.
        """
        return GlobalCache._cache.get(key, None)

    @staticmethod
    def set(key, value):
        """
        Set the value of the key in the cache.
        If the key does not exist, create it.
        """
        GlobalCache._cache[key] = value

    @staticmethod
    def clear():
        """
        Clear the cache.
        """
        GlobalCache._cache = {}




