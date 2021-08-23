import uuid


def generate_expressions(updates={},deletes=[]):
    # Build expression_attribute_names by assigning str(uuid.uuid4()) to each key
    expression_attribute_names = {}
    expression_attribute_values = {}

    expression_attribute_values_inverted = {}
    

    update_expressions = []

    for real_attribute_name in updates:
        uuid_name = ("#"+str(uuid.uuid4())).replace("-","")

        uuid_values = [
            
        ]

        real_values = [

        ]

        

        
        expression_attribute_names[uuid_name] = real_attribute_name
        
        string_update_value = ""

        if type(updates[real_attribute_name]) == list:    
            real_value = updates[real_attribute_name][0]
            real_values.append(real_value)
            if real_value in expression_attribute_values_inverted:
                # Avoid generating new UUID. Save bandwith by using existing UUID
                uuid_value = expression_attribute_values_inverted[real_value] 
            else:
                uuid_value = (":"+str(uuid.uuid4())).replace("-","")
                expression_attribute_values[uuid_value] = real_value
            uuid_values.append(uuid_value)
            
            string_update_value = "%s = if_not_exists(%s, %s)" % (uuid_name, uuid_name, uuid_values[0])

            if len(updates[real_attribute_name]) > 1:
                real_value = updates[real_attribute_name][1]
                real_values.append(real_value)
                if real_value in expression_attribute_values_inverted:
                    # Avoid generating new UUID. Save bandwith by using existing UUID
                    uuid_value = expression_attribute_values_inverted[real_value] 
                else:
                    uuid_value = (":"+str(uuid.uuid4())).replace("-","")
                    expression_attribute_values[uuid_value] = real_value
                uuid_values.append(uuid_value)
                string_update_value += " + " + uuid_values[1]
        else:
            real_value = updates[real_attribute_name]
            real_values.append(real_value)
            if real_value in expression_attribute_values_inverted:
                # Avoid generating new UUID. Save bandwith by using existing UUID
                uuid_value = expression_attribute_values_inverted[real_value] 
            else:
                uuid_value = (":"+str(uuid.uuid4())).replace("-","")
                expression_attribute_values[uuid_value] = real_value
            uuid_values.append(uuid_value)
            string_update_value = "%s = %s" % (uuid_name, uuid_values[0])


        update_expressions.append(string_update_value)
        
        
    
        
    update_expression_query = "SET " + ", ".join(update_expressions)


    if len(deletes):
        update_expression_query += " REMOVE " + ", ".join(deletes)

    return update_expression_query, expression_attribute_names, expression_attribute_values, 