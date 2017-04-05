local json = require "lib/json"

function process_metric(pyld)
end

function process_log(pyld)
   local line = payload.log_value(pyld, 1)
   local json_pyld = json.decode(line)
   payload.log_set_field(pyld, 1, "foo", json_pyld["foo"])
end

function tick(pyld)
end
