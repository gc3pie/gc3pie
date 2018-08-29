function gcsvBulk_wrapper(function_name, csvFile, limit, offset)
%MATLAB_CSV_WRAPPER executes a given function with a given csv file with each line as
% parameters for the function. The parameters of the csv are limited by limit and are
% offseted by offset

    %% add function to execute here:
    assert(exist('function_name', 'var') == 1, ...
        'matlab_csv_wrapper:function_name:noInput', ...
        'No function specified');

    if isa(function_name, 'function_handle')
       func = function_name;
    else
        assert(ischar(function_name), ...
            'matlab_csv_wrapper:function_name:invalidType', ...
            'function name must be a string');

        assert(exist(function_name, 'file') == 2 || ...
            exist(function_name, 'builtin') == 5, ...
            'matlab_csv_wrapper:function_name:invalidFunction', ...
            'Function specified not found');

        func = str2func(function_name);
    end

    %%
    assert(exist('csvFile', 'var') == 1, ...
        'matlab_csv_wrapper:csvFile:noInputArgument', ...
        'No csv file to process specified');

    assert(exist(csvFile, 'file') == 2, 'matlab_csv_wrapper:invalidFileName', ...
        'Invalid filename specified' );

    params = csvread(csvFile);

    assert(all(isnumeric(params)), 'matlab_csv_wrapper:invalidCSV', ...
        'Only nummeric data is allowed in the csv for the time being');

    assert(nargin(func) == size(params, 2), 'matlab_csv_wrapper:invalidCSV', ...
        'Invalid csv, to many inputs for function %s', func2str(func));

    %%
    nrJobs = size(params, 1);
    if ~exist('limit', 'var');
        limit = nrJobs;
    else
      if ischar(limit), limit = str2double(limit); end
    end

    if ~exist('offset', 'var');
        offset = 0;
    else
        if ischar(offset), offset = str2double(offset); end
    end

    assert(isnumeric(limit) && isscalar(limit) && isreal(limit) && ge(limit, 0), ...
        'matlab_csv_wrapper:limit', ...
        'Limit must be numeric, scalar, real and positiv');

    assert(isnumeric(offset) && isscalar(offset) && isreal(offset) && ge(offset, 0), ...
        'matlab_csv_wrapper:offset', ...
        'Offset must be numeric, scalar, real and positiv');

    %% process file
    start = 1 + offset;
    start = min(start, nrJobs);
    toEnd = start + limit;
    toEnd = min(toEnd, nrJobs);
    for currentParam = start:toEnd
        input = num2cell(params(currentParam, :));
        func(input{:});
    end

end
