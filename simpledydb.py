import uuid
import re


def generate_attribute_name():
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return "#"+re.sub(r'[^a-zA-Z0-9]', '', str(uuid.uuid4()))
    #return a


def generate_attribute_name_value():
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return ":"+re.sub(r'[^a-zA-Z0-9]', '', str(uuid.uuid4()))
    #return b

def generate_expressions(updates={}, deletes=[]):
    print(updates)
    # Build expression_attribute_names, expression_attribute_values by assigning str(uuid.uuid4()) to each key
    # update_expressions should represent UpdateExpression for dynamodb attribute
    if not len(updates) and not len(deletes):
        raise "No updates"
    expression_attribute_names = {}
    expression_attribute_values = {}

    expression_attribute_values_inverted = {}

    update_expressions = []

    for real_name in updates:
        # try:
        this_expression_attribute_name = generate_attribute_name()

        this_expression_attribute_values = [

        ]

        this_real_value = [

        ]
        expression_attribute_names[this_expression_attribute_name] = real_name
        string_update_value = ""

        if type(updates[real_name]) == list and len(updates[real_name]) > 0:
            real_value = updates[real_name][0]
            is_list_append = False
            if type(real_value) == list:
                is_list_append = True
            
            if not is_list_append:
                if real_value in expression_attribute_values_inverted:
                    # Avoid generating new UUID. Save bandwith by using existing UUID
                    expr_attr_value_key = expression_attribute_values_inverted[real_value]
                    expression_attribute_values[expr_attr_value_key] = real_value
                else:
                    expr_attr_value_key = generate_attribute_name_value()
                    expression_attribute_values_inverted[real_value] = expr_attr_value_key
                    expression_attribute_values[expr_attr_value_key] = real_value
                this_expression_attribute_values.append(real_value)
                string_update_value = "%s = if_not_exists(%s, %s)" % (
                    this_expression_attribute_name, this_expression_attribute_name, expr_attr_value_key)
            else:
                expr_attr_value_key = generate_attribute_name_value()
                expression_attribute_values[expr_attr_value_key] = real_value
                this_expression_attribute_values.append(expr_attr_value_key)
                empty_list_value = []
                expr_attr_value_key_empty_list = generate_attribute_name_value()
                expression_attribute_values[expr_attr_value_key_empty_list] = empty_list_value
                # #exceptions = list_append(if_not_exists(#exceptions,:empty_list),:err)
                string_update_value = "%s = list_append(if_not_exists(%s, %s), %s)" % (
                    this_expression_attribute_name, this_expression_attribute_name, expr_attr_value_key_empty_list, expr_attr_value_key)
            if len(updates[real_name]) > 1:
                real_value = updates[real_name][1]
                
                if real_value in expression_attribute_values_inverted:
                    # Avoid generating new UUID. Save bandwith by using existing UUID
                    expr_attr_value_key = expression_attribute_values_inverted[real_value]
                else:
                    expr_attr_value_key = generate_attribute_name_value()
                    expression_attribute_values[expr_attr_value_key] = real_value
                this_expression_attribute_values.append(real_value)
                string_update_value += " + " + \
                    expr_attr_value_key
        else:
            real_value = updates[real_name]
            
            if repr(updates[real_name]) in expression_attribute_values_inverted:
                # Avoid generating new UUID. Save bandwith by using existing UUID
                expr_attr_value_key = expression_attribute_values_inverted[real_value]
            else:
                expr_attr_value_key = generate_attribute_name_value()
                expression_attribute_values[expr_attr_value_key] = real_value
            this_expression_attribute_values.append(real_value)
            string_update_value = "%s = %s" % (
                this_expression_attribute_name, expr_attr_value_key)

        update_expressions.append(string_update_value)
        # except Exception as ex:
        #     print("Exception in key %s" % real_name)
        #     raise ex

    update_expression_query = "SET " + ", ".join(update_expressions)

    if len(deletes):
        update_expression_query += " REMOVE " + ", ".join(deletes)

    return update_expression_query, expression_attribute_names, expression_attribute_values,




# a = {'cp_last_done_age': 1630422514}


# x,y,z= generate_expressions(a)

# import boto3

# boto3.resource('dynamodb').Table('tracked_urls').update_item(
#     Key= {'url': 'https://eurojobs.com/browse-by-country/asdzcxzxccxcx/'},
#     UpdateExpression=x,
#     ExpressionAttributeNames=y,
#     ExpressionAttributeValues=z,
# )
