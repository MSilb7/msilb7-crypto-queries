import pandas as pd

# Set the number you want to format
x = 50000000

# Format the number as a string with a currency symbol and abbreviated magnitude
x_mil = x /1e6
formatted_number = '${:,.1f}M'.format(x_mil)

# Print the formatted number
print(formatted_number)