function process_metric(pyld)
end

function process_log(pyld)
   local path = payload.log_path(pyld, 1)
   payload.log_set_field(pyld, 1, "foo", path)
end

function tick(pyld)
end
