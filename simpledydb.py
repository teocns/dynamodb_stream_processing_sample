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

    for real_name in updates:
        expr_attr_name = generate_attribute_name(real_name)

        expr_attribute_values = [

        ]

        real_values = [

        ]
        expression_attribute_names[expr_attr_name] = real_name
        string_update_value = ""

        if type(updates[real_name]) == list and len(updates[real_name]) > 0:
            real_value = updates[real_name][0]
            is_list_append = False
            if type(real_value) == list:
                is_list_append = True
            real_values.append(real_value)
            if not is_list_append:
                if real_value in expression_attribute_values_inverted:
                    # Avoid generating new UUID. Save bandwith by using existing UUID
                    expr_attr_value_key = expression_attribute_values_inverted[real_value]
                else:
                    expr_attr_value_key = generate_attribute_name_value(real_name)
                    expression_attribute_values_inverted[real_value] = expr_attr_value_key
                    expression_attribute_values[expr_attr_value_key] = real_value
                string_update_value = "%s = if_not_exists(%s, %s)" % (
                    expr_attr_name, expr_attr_name, expr_attribute_values[0])
            else:
                expr_attr_value_key = generate_attribute_name_value(real_name)
                expression_attribute_values[expr_attr_value_key] = real_value
                expr_attribute_values.append(expr_attr_value_key)
                empty_list_value = []
                expr_attr_value_key = generate_attribute_name_value(real_name) + "_empty"
                expression_attribute_values[expr_attr_value_key] = empty_list_value
                # #exceptions = list_append(if_not_exists(#exceptions,:empty_list),:err)
                string_update_value = "%s = list_append(if_not_exists(%s, %s), %s)" % (
                    expr_attr_name, expr_attr_name, expr_attr_value_key, expr_attribute_values[0])
            if len(updates[real_name]) > 1:
                real_value = updates[real_name][1]
                real_values.append(real_value)
                if real_value in expression_attribute_values_inverted:
                    # Avoid generating new UUID. Save bandwith by using existing UUID
                    expr_attr_value_key = expression_attribute_values_inverted[real_value]
                else:
                    expr_attr_value_key = generate_attribute_name_value(real_name) + '_incr'
                    expression_attribute_values[expr_attr_value_key] = real_value
                expr_attribute_values.append(expr_attr_value_key)
                string_update_value += " + " + expr_attribute_values[1]
        else:
            real_value = updates[real_name]
            real_values.append(real_value)
            if repr(updates[real_name]) in expression_attribute_values_inverted:
                # Avoid generating new UUID. Save bandwith by using existing UUID
                expr_attr_value_key = expression_attribute_values_inverted[real_value]
            else:
                expr_attr_value_key = generate_attribute_name_value(real_name)
                expression_attribute_values[expr_attr_value_key] = real_value
            expr_attribute_values.append(expr_attr_value_key)
            string_update_value = "%s = %s" % (expr_attr_name, expr_attribute_values[0])

        update_expressions.append(string_update_value)

    update_expression_query = "SET " + ", ".join(update_expressions)

    if len(deletes):
        update_expression_query += " REMOVE " + ", ".join(deletes)

    return update_expression_query, expression_attribute_names, expression_attribute_values,
