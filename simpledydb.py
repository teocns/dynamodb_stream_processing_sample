import uuid
import re

def generate_attribute_name(real_name):    
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return "#"+re.sub(r'[^a-zA-Z0-9_\-\.]', '', real_name)

def generate_attribute_name_value(real_name):    
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return ":"+re.sub(r'[^a-zA-Z0-9_\-\.]', '', real_name)




def generate_expressions(updates={}, deletes=[]):
    # Build expression_attribute_names, expression_attribute_values by assigning str(uuid.uuid4()) to each key
    # update_expressions should represent UpdateExpression for dynamodb attribute
    if not len(updates) and not len(deletes):
        raise "No updates"
    expression_attribute_names = {}
    expression_attribute_values = {}

    expression_attribute_values_inverted = {}

    update_expressions = []

    for real_attribute_name in updates:
        uuid_name = generate_attribute_name(real_attribute_name)

        uuid_values = [

        ]

        real_values = [

        ]

        expression_attribute_names[uuid_name] = real_attribute_name

        string_update_value = ""

        if type(updates[real_attribute_name]) == list and len(updates[real_attribute_name]) > 0:
            real_value = updates[real_attribute_name][0]
            is_list_append = False
            if type(real_value) == list:
                is_list_append = True
            real_values.append(real_value)
            if real_value in expression_attribute_values_inverted:
                # Avoid generating new UUID. Save bandwith by using existing UUID
                uuid_value = expression_attribute_values_inverted[real_value]
            else:
                uuid_value = generate_attribute_name_value(real_attribute_name)
                expression_attribute_values[uuid_value] = real_value
            uuid_values.append(uuid_value)

            # Real value could be a list_append
            if is_list_append:
                empty_list_value = []
                empty_list_uuid = generate_attribute_name_value(real_attribute_name)
                expression_attribute_values[empty_list_uuid] = empty_list_value
                # #exceptions = list_append(if_not_exists(#exceptions,:empty_list),:err)
                string_update_value = "%s = list_append(if_not_exists(%s, %s), %s)" % (
                    uuid_name, uuid_name, empty_list_uuid, uuid_values[0])

            else:
                string_update_value = "%s = if_not_exists(%s, %s)" % (
                    uuid_name, uuid_name, uuid_values[0])
            if len(updates[real_attribute_name]) > 1:
                real_value = updates[real_attribute_name][1]
                real_values.append(real_value)
                if real_value in expression_attribute_values_inverted:
                    # Avoid generating new UUID. Save bandwith by using existing UUID
                    uuid_value = expression_attribute_values_inverted[real_value]
                else:
                    uuid_value = generate_attribute_name_value(real_attribute_name)
                    expression_attribute_values[uuid_value] = real_value
                uuid_values.append(uuid_value)
                string_update_value += " + " + uuid_values[1]
        else:
            real_value = updates[real_attribute_name]
            real_values.append(real_value)
            if repr(updates[real_attribute_name]) in expression_attribute_values_inverted:
                # Avoid generating new UUID. Save bandwith by using existing UUID
                uuid_value = expression_attribute_values_inverted[real_value]
            else:
                uuid_value = generate_attribute_name_value(real_attribute_name)
                expression_attribute_values[uuid_value] = real_value
            uuid_values.append(uuid_value)
            string_update_value = "%s = %s" % (uuid_name, uuid_values[0])

        update_expressions.append(string_update_value)

    update_expression_query = "SET " + ", ".join(update_expressions)

    if len(deletes):
        update_expression_query += " REMOVE " + ", ".join(deletes)

    return update_expression_query, expression_attribute_names, expression_attribute_values,
