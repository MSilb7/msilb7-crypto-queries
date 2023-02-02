# https://www.thepythoncode.com/article/convert-pandas-dataframe-to-html-table-python
import pandas as pd
def generate_html(dataframe: pd.DataFrame):
    # get the table HTML from the dataframe
    table_html = dataframe.to_html(table_id="table")
    # construct the complete HTML with jQuery Data tables
    # You can disable paging or enable y scrolling on lines 20 and 21 respectively
    html = f"""
    <html>
    <header>
        <link href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css" rel="stylesheet">
    </header>
    <body>
    {table_html}
    <script src="https://code.jquery.com/jquery-3.6.0.slim.min.js" integrity="sha256-u7e5khyithlIdTpu22PHhENmPcRdFiHRjhAuHcs05RI=" crossorigin="anonymous"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
    <script>
        $(document).ready( function () {{
            $('#table').DataTable({{
                // paging: false,    
                // scrollY: 400,
                pageLength: 25
            }});
        }});
    </script>
    </body>
    </html>
    """
    # return the html
    return html


# https://nbviewer.org/gist/alubbock/e5d915397179b9626ae63a55244f510d
import uuid
import numpy

def DataTable(df):
    """ Prints a pandas.DataFrame using jQuery DataTable plugin """
    from IPython.display import HTML
    output = """<div id="datatable-%(uuid)s">%(html)s
            <script type="text/javascript">
                $(document).ready(function() {
                    require(['dataTables'], function() {
                        $('#datatable-%(uuid)s').find('table.datatable').dataTable({
                        columnDefs: [{ targets: %(sci_cols)s, type: 'scientific' }]});
                    });
                });
            </script>
        </div>
    """ % {'uuid': uuid.uuid1(), 'html': df.to_html(classes="datatable display"),
          'sci_cols': '[%s]' % ",".join([str(i) for i, _ in enumerate(df.dtypes == numpy.float64)])}
    return HTML(output)

def format_num(x, prepend):
    try:
        x = float(x)
        if x >= 1e6:
            mil_format = prepend + '{:.1f}M'
            return mil_format.format(x / 1e6)
        elif x >= 1e3:
            k_format = prepend + '{:.1f}k'
            return k_format.format(x / 1e3)
        else:
            return x
    except:
        return x
    
def format_num(x, prefix=''):
    if x == '': #if null, make it 0
        x=0
    x = float(x) #just cast for safety
    if x >= 1e6:
        return f'{prefix}{x / 1e6:,.1f}M'
    elif x >= 1e3:
        return f'{x / 1e3:,.1f}k'
    if x < -1e6:
        return f'-{prefix}{abs(x) / 1e6:,.1f}M'
    elif x < -1e3:
        return f'-{prefix}{abs(x) / 1e3:,.1f}k'
    elif x < 0:
        return f'-{prefix}{x:,.1f}'
    
    return f'{prefix}{x:,.1f}'
def format_pct(x):
    if x == '': #if null, return it
        return x
    else:
        x = float(x) #just cast for safety
        return '{:.1%}'.format(x)
    