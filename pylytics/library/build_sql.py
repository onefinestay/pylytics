"""
A class to easily generate a MySQL CREATE query,
knowing the columns names, types, unique key and foreign key contraints

"""


class SQLBuilder(object):
    def __init__(self, table_name, cols_names, cols_types, unique_key=None, foreign_keys=None):
        self.table_name = table_name
        self.cols_names = cols_names
        self.cols_types = cols_types
        self.unique_key = unique_key
        self.foreign_keys = foreign_keys
        self.query = self._get_query()
        
    def _get_query(self):
        """
        Builds and returns the CREATE query
        
        """
        query = 'CREATE TABLE %s (\n' % self.table_name
        
        query += '  `id` INT(11) NOT NULL AUTO_INCREMENT'        
        for col in self.cols_names:
            query += '  ,`%s` %s DEFAULT NULL' % (col, self.cols_types[col])
        query += '   ,`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        query += '  ,PRIMARY KEY (`id`)'
        
        if self.unique_key is not None :
            query += '  ,UNIQUE KEY `%s_uk` (%s)' % (self.table_name, ','.join(['`'+e+'`' for e in self.unique_key]))
        
        if self.foreign_keys is not None :
            for i,(fk,ref) in enumerate(self.foreign_keys):
                query += '  ,CONSTRAINT `%s_ibfk_%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`id`)' % (self.table_name, i, fk, ref)

        query += ') ENGINE=INNODB DEFAULT CHARSET=utf8'
        
        return query
