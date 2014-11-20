
'''
A security definition can be extensive:
NY_account = SecurityRole("NY_account", schema_permission)
NY_account_cube = NY_account.addCube("Sales", cube_permission)
NY_account_cube_dimension = NY_account_cube.addDimension("[Measures]", dimension_permission)
NY_account_cube_hierarchy = NY_account_cube.addHierarchy("[Store]", hierarchy_permission)
NY_account_cube_hierarchy_member = NY_account_cube_hierarchy.addMember("[Store].[USA].[CA]", hierarchy_permission)

or simplified:

NY_account = SimplePermission("Sales", "[Store].[USA].[CA]") 
'''

def set_role():
    ''' Should always return a SecurityRole'''
    return(simplePermission("example", "[Store].[USA].[CA]"))
