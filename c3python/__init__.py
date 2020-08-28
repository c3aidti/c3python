__all__ = [ 'get_c3' , 'EvaluateResultToPandas']
__version__ = '0.2'

def get_c3(url, tenant, tag, mode='thick', define_types=True, auth=None):
    """
    Returns c3remote type system for python.
    """
    try:
        from urllib.request import urlopen
    except ImportError:
        from urllib2 import urlopen
    from types import ModuleType
    c3iot = ModuleType('c3IoT')
    c3iot.__loader__ = c3iot
    src = urlopen(url + '/public/python/c3remote_bootstrap.py').read()
    exec(src, c3iot.__dict__)
    return c3iot.C3RemoteLoader.typeSys(
        url=url,
        tenant=tenant,
        tag=tag,
        mode=mode,
        auth=auth,
        define_types=define_types
    )

def EvaluateResultToPandas(result=None, eval_spec=None):
    """
    Take an EvaluateResult type and build a usable Pandas DataFrame from it.
    If you pass the EvaluateSpec you used to eval_spec (either c3.EvaluateSpec
    or dict type), this function will name the columns correctly as well.

    Arguments:
      result: A c3.EvaluateResult type which contains the results you want to
        turn into a Pandas DataFrame. [Required]
      eval_spec: Either a dict or c3.EvaluateSpec containing the spec you used
        to get the results. If no eval_spec is passed, by default the columns
        will be named 'c0', 'c1', ...

    Returns:
      A Pandas DataFrame containing the EvaluateResult data.
    """

    # Check whether the input is None
    if result is None:
        raise ValueError("You must pass a non-None value.")

    # Check whether the input is the right type
    # For now, we do a string comparison because the 'c3' object
    # Is not in scope in this function.
    if str(type(result)) != 'c3.EvaluateResult':
        raise ValueError("You must pass a c3.EvaluateResult type.")

    # Find number of columns in the result
    num_columns = len(result.tuples[0].cells)
    
    # Get column names
    if eval_spec is None:
        column_names = ['c{}'.format(i) for i in range(num_columns)]
    else:
        if type(eval_spec) is dict:
            column_names = eval_spec['projection'].split(',')
        elif str(type(eval_spec)) == 'c3.EvaluateSpec':
            # For now, we do a string comparison because the 'c3' object
            # Is not in scope in this function.
            column_names = eval_spec.projection.split(',')
        else:
            raise RuntimeError("eval_spec should be either a dict or c3.EvaluateSpec, Type {} not supported".format(type(eval_spec)))

        if len(column_names) != num_columns:
            raise RuntimeError("Eval Spec doesn't have correct number of columns! Has {} expected {}. Did you use the right Spec?".format(len(column_names), num_columns))

    # Initialize the results dict
    results = {}
    for col in column_names:
        results[col] = []
    
    # Fill the results dict
    for row in result.tuples:
        for i in range(num_columns):
            results[column_names[i]].append(row.cells[i].value())
    
    # Build and return the final Pandas DataFrame
    try:
        import pandas as pd
        return pd.DataFrame(results) 
    except ImportError as e:
        # Trick from https://stackoverflow.com/questions/6062576/adding-information-to-an-exception/6062799
        raise type(e)(e.message + ' EvaluateResultToPandas needs pandas to work!')
    except Exception as e:
        raise e
