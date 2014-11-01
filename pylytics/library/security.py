'''
Full security rules for mondrian schema can be found here: 
http://mondrian.pentaho.com/documentation/schema.php#Access_control

These classes can be left agnostic to the olap engine, but looking at 
mondrian schema is a good starting point.

Security tree:
-Role
    -Cube
        -Dimension
        -Hierarchy
            -Member
'''

class SecurityRole(object):
    ''' SecurityRole class that defines security for a given role name
        A Role can have several cubes
    '''
    def __init__(self, name, permission):
        ''' permission can be: 'all' or 'none' '''
        self.name = name
        self.permission = permission
        self.cubes = []

    def addCube(self, cube_name, permission):
        ''' Creates a SecurityCube object and store it on self.cubes '''
        pass


class SecurityCube(object):
    ''' Cube security definition for a role 
        A Cube can have Dimensions and Hierarchies
    '''
    def __init__(self, name, permission):
        ''' permission can be: 'all' or 'none' '''
        self.name = name
        self.permission = permission
        self.dimensions = []
        self.hierarquies = []

    def addDimension(self, dimension_name, permission):
        ''' Creates a Dimension object and store it on self.dimensions'''
        pass

    def addHierarchy(self, hierarchy_name, permission):
        ''' Creates a Hierarchy object and store it on self.hierarchies'''
        pass


class SecurityDimension(object):
    ''' Dimension security definition for a role 
    '''
    def __init__(self, name, permission):
        ''' permission can be: 'all', 'custom' or 'none' '''
        self.name = name
        self.permission = permission
        self.members = []


class SecurityHierarchy(object):
    ''' Hierarchy security definition for a role 
    '''
    def __init__(self, name, permission):
        ''' permission can be: 'all', 'custom' or 'none' '''
        self.name = name
        self.permission = permission

    def addMember(self, member_name, permission):
        ''' Creates a Member object and store it on self.members'''
        pass


class SecurityMember(object):
    ''' Member security definition for a role 
    '''
    def __init__(self, name, permission):
        ''' permission can be: 'all' or 'none' '''
        self.name = name
        self.permission = permission
        self.members = []




'''
1. Members inherit access from their parents. If you deny access to California, you won't be able to see San Francisco.
2. Grants are order-dependent. If you grant access to USA, then deny access to Oregon, then you won't be able to see Oregon, or Portland. But if you were to deny access to Oregon, then grant access to USA, you can effectively see everything.
3. A member is visible if any of its children are visible. Suppose you deny access to USA, then grant access to California. You will be able to see USA, and California, but none of the other states. The totals against USA will still reflect all states, however. If the parent HierarchyGrant specifies a top level, only the parents equal or below this level will be visible. Similarly, if a bottom level is specified, only the children above or equal to the level are visible.
4. Member grants don't override the hierarchy grant's top- and bottom-levels. If you set topLevel="[Store].[Store State]", and grant access to California, you won't be able to see USA. Member grants do not override the topLevel and bottomLevel attributes. You can grant or deny access to a member of any level, but the top and bottom constraints have precedence on the explicit member grants.
'''
