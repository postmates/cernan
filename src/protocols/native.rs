// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(PartialEq,Clone,Default)]
pub struct Payload {
    // message fields
    points: ::protobuf::RepeatedField<Telemetry>,
    lines: ::protobuf::RepeatedField<LogLine>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Payload {}

impl Payload {
    pub fn new() -> Payload {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Payload {
        static mut instance: ::protobuf::lazy::Lazy<Payload> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Payload,
        };
        unsafe {
            instance.get(Payload::new)
        }
    }

    // repeated .com.postmates.cernan.Telemetry points = 2;

    pub fn clear_points(&mut self) {
        self.points.clear();
    }

    // Param is passed by value, moved
    pub fn set_points(&mut self, v: ::protobuf::RepeatedField<Telemetry>) {
        self.points = v;
    }

    // Mutable pointer to the field.
    pub fn mut_points(&mut self) -> &mut ::protobuf::RepeatedField<Telemetry> {
        &mut self.points
    }

    // Take field
    pub fn take_points(&mut self) -> ::protobuf::RepeatedField<Telemetry> {
        ::std::mem::replace(&mut self.points, ::protobuf::RepeatedField::new())
    }

    pub fn get_points(&self) -> &[Telemetry] {
        &self.points
    }

    fn get_points_for_reflect(&self) -> &::protobuf::RepeatedField<Telemetry> {
        &self.points
    }

    fn mut_points_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Telemetry> {
        &mut self.points
    }

    // repeated .com.postmates.cernan.LogLine lines = 3;

    pub fn clear_lines(&mut self) {
        self.lines.clear();
    }

    // Param is passed by value, moved
    pub fn set_lines(&mut self, v: ::protobuf::RepeatedField<LogLine>) {
        self.lines = v;
    }

    // Mutable pointer to the field.
    pub fn mut_lines(&mut self) -> &mut ::protobuf::RepeatedField<LogLine> {
        &mut self.lines
    }

    // Take field
    pub fn take_lines(&mut self) -> ::protobuf::RepeatedField<LogLine> {
        ::std::mem::replace(&mut self.lines, ::protobuf::RepeatedField::new())
    }

    pub fn get_lines(&self) -> &[LogLine] {
        &self.lines
    }

    fn get_lines_for_reflect(&self) -> &::protobuf::RepeatedField<LogLine> {
        &self.lines
    }

    fn mut_lines_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<LogLine> {
        &mut self.lines
    }
}

impl ::protobuf::Message for Payload {
    fn is_initialized(&self) -> bool {
        for v in &self.points {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.lines {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.points)?;
                },
                3 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.lines)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.points {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.lines {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.points {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.lines {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Payload {
    fn new() -> Payload {
        Payload::new()
    }

    fn descriptor_static(_: ::std::option::Option<Payload>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Telemetry>>(
                    "points",
                    Payload::get_points_for_reflect,
                    Payload::mut_points_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<LogLine>>(
                    "lines",
                    Payload::get_lines_for_reflect,
                    Payload::mut_lines_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Payload>(
                    "Payload",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Payload {
    fn clear(&mut self) {
        self.clear_points();
        self.clear_lines();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Payload {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct LogLine {
    // message fields
    path: ::protobuf::SingularField<::std::string::String>,
    value: ::protobuf::SingularField<::std::string::String>,
    pub metadata: ::std::collections::HashMap<::std::string::String, ::std::string::String>,
    timestamp_ms: ::std::option::Option<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for LogLine {}

impl LogLine {
    pub fn new() -> LogLine {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static LogLine {
        static mut instance: ::protobuf::lazy::Lazy<LogLine> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const LogLine,
        };
        unsafe {
            instance.get(LogLine::new)
        }
    }

    // optional string path = 1;

    pub fn clear_path(&mut self) {
        self.path.clear();
    }

    pub fn has_path(&self) -> bool {
        self.path.is_some()
    }

    // Param is passed by value, moved
    pub fn set_path(&mut self, v: ::std::string::String) {
        self.path = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_path(&mut self) -> &mut ::std::string::String {
        if self.path.is_none() {
            self.path.set_default();
        }
        self.path.as_mut().unwrap()
    }

    // Take field
    pub fn take_path(&mut self) -> ::std::string::String {
        self.path.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_path(&self) -> &str {
        match self.path.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_path_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.path
    }

    fn mut_path_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.path
    }

    // optional string value = 2;

    pub fn clear_value(&mut self) {
        self.value.clear();
    }

    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    // Param is passed by value, moved
    pub fn set_value(&mut self, v: ::std::string::String) {
        self.value = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_value(&mut self) -> &mut ::std::string::String {
        if self.value.is_none() {
            self.value.set_default();
        }
        self.value.as_mut().unwrap()
    }

    // Take field
    pub fn take_value(&mut self) -> ::std::string::String {
        self.value.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_value(&self) -> &str {
        match self.value.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_value_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.value
    }

    fn mut_value_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.value
    }

    // repeated .com.postmates.cernan.LogLine.MetadataEntry metadata = 3;

    pub fn clear_metadata(&mut self) {
        self.metadata.clear();
    }

    // Param is passed by value, moved
    pub fn set_metadata(&mut self, v: ::std::collections::HashMap<::std::string::String, ::std::string::String>) {
        self.metadata = v;
    }

    // Mutable pointer to the field.
    pub fn mut_metadata(&mut self) -> &mut ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &mut self.metadata
    }

    // Take field
    pub fn take_metadata(&mut self) -> ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        ::std::mem::replace(&mut self.metadata, ::std::collections::HashMap::new())
    }

    pub fn get_metadata(&self) -> &::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &self.metadata
    }

    fn get_metadata_for_reflect(&self) -> &::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &self.metadata
    }

    fn mut_metadata_for_reflect(&mut self) -> &mut ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &mut self.metadata
    }

    // optional int64 timestamp_ms = 4;

    pub fn clear_timestamp_ms(&mut self) {
        self.timestamp_ms = ::std::option::Option::None;
    }

    pub fn has_timestamp_ms(&self) -> bool {
        self.timestamp_ms.is_some()
    }

    // Param is passed by value, moved
    pub fn set_timestamp_ms(&mut self, v: i64) {
        self.timestamp_ms = ::std::option::Option::Some(v);
    }

    pub fn get_timestamp_ms(&self) -> i64 {
        self.timestamp_ms.unwrap_or(0)
    }

    fn get_timestamp_ms_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.timestamp_ms
    }

    fn mut_timestamp_ms_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.timestamp_ms
    }
}

impl ::protobuf::Message for LogLine {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.path)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.value)?;
                },
                3 => {
                    ::protobuf::rt::read_map_into::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(wire_type, is, &mut self.metadata)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.timestamp_ms = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.path.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.value.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        my_size += ::protobuf::rt::compute_map_size::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(3, &self.metadata);
        if let Some(v) = self.timestamp_ms {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.path.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.value.as_ref() {
            os.write_string(2, &v)?;
        }
        ::protobuf::rt::write_map_with_cached_sizes::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(3, &self.metadata, os)?;
        if let Some(v) = self.timestamp_ms {
            os.write_int64(4, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for LogLine {
    fn new() -> LogLine {
        LogLine::new()
    }

    fn descriptor_static(_: ::std::option::Option<LogLine>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "path",
                    LogLine::get_path_for_reflect,
                    LogLine::mut_path_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "value",
                    LogLine::get_value_for_reflect,
                    LogLine::mut_value_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_map_accessor::<_, ::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(
                    "metadata",
                    LogLine::get_metadata_for_reflect,
                    LogLine::mut_metadata_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "timestamp_ms",
                    LogLine::get_timestamp_ms_for_reflect,
                    LogLine::mut_timestamp_ms_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<LogLine>(
                    "LogLine",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for LogLine {
    fn clear(&mut self) {
        self.clear_path();
        self.clear_value();
        self.clear_metadata();
        self.clear_timestamp_ms();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for LogLine {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for LogLine {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Telemetry {
    // message fields
    name: ::protobuf::SingularField<::std::string::String>,
    samples: ::std::vec::Vec<f64>,
    persisted: ::std::option::Option<bool>,
    method: ::std::option::Option<AggregationMethod>,
    pub metadata: ::std::collections::HashMap<::std::string::String, ::std::string::String>,
    timestamp_ms: ::std::option::Option<i64>,
    bin_bounds: ::std::vec::Vec<f64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Telemetry {}

impl Telemetry {
    pub fn new() -> Telemetry {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Telemetry {
        static mut instance: ::protobuf::lazy::Lazy<Telemetry> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Telemetry,
        };
        unsafe {
            instance.get(Telemetry::new)
        }
    }

    // optional string name = 1;

    pub fn clear_name(&mut self) {
        self.name.clear();
    }

    pub fn has_name(&self) -> bool {
        self.name.is_some()
    }

    // Param is passed by value, moved
    pub fn set_name(&mut self, v: ::std::string::String) {
        self.name = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_name(&mut self) -> &mut ::std::string::String {
        if self.name.is_none() {
            self.name.set_default();
        }
        self.name.as_mut().unwrap()
    }

    // Take field
    pub fn take_name(&mut self) -> ::std::string::String {
        self.name.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_name(&self) -> &str {
        match self.name.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_name_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.name
    }

    fn mut_name_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.name
    }

    // repeated double samples = 2;

    pub fn clear_samples(&mut self) {
        self.samples.clear();
    }

    // Param is passed by value, moved
    pub fn set_samples(&mut self, v: ::std::vec::Vec<f64>) {
        self.samples = v;
    }

    // Mutable pointer to the field.
    pub fn mut_samples(&mut self) -> &mut ::std::vec::Vec<f64> {
        &mut self.samples
    }

    // Take field
    pub fn take_samples(&mut self) -> ::std::vec::Vec<f64> {
        ::std::mem::replace(&mut self.samples, ::std::vec::Vec::new())
    }

    pub fn get_samples(&self) -> &[f64] {
        &self.samples
    }

    fn get_samples_for_reflect(&self) -> &::std::vec::Vec<f64> {
        &self.samples
    }

    fn mut_samples_for_reflect(&mut self) -> &mut ::std::vec::Vec<f64> {
        &mut self.samples
    }

    // optional bool persisted = 3;

    pub fn clear_persisted(&mut self) {
        self.persisted = ::std::option::Option::None;
    }

    pub fn has_persisted(&self) -> bool {
        self.persisted.is_some()
    }

    // Param is passed by value, moved
    pub fn set_persisted(&mut self, v: bool) {
        self.persisted = ::std::option::Option::Some(v);
    }

    pub fn get_persisted(&self) -> bool {
        self.persisted.unwrap_or(false)
    }

    fn get_persisted_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.persisted
    }

    fn mut_persisted_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.persisted
    }

    // optional .com.postmates.cernan.AggregationMethod method = 4;

    pub fn clear_method(&mut self) {
        self.method = ::std::option::Option::None;
    }

    pub fn has_method(&self) -> bool {
        self.method.is_some()
    }

    // Param is passed by value, moved
    pub fn set_method(&mut self, v: AggregationMethod) {
        self.method = ::std::option::Option::Some(v);
    }

    pub fn get_method(&self) -> AggregationMethod {
        self.method.unwrap_or(AggregationMethod::SUMMARIZE)
    }

    fn get_method_for_reflect(&self) -> &::std::option::Option<AggregationMethod> {
        &self.method
    }

    fn mut_method_for_reflect(&mut self) -> &mut ::std::option::Option<AggregationMethod> {
        &mut self.method
    }

    // repeated .com.postmates.cernan.Telemetry.MetadataEntry metadata = 5;

    pub fn clear_metadata(&mut self) {
        self.metadata.clear();
    }

    // Param is passed by value, moved
    pub fn set_metadata(&mut self, v: ::std::collections::HashMap<::std::string::String, ::std::string::String>) {
        self.metadata = v;
    }

    // Mutable pointer to the field.
    pub fn mut_metadata(&mut self) -> &mut ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &mut self.metadata
    }

    // Take field
    pub fn take_metadata(&mut self) -> ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        ::std::mem::replace(&mut self.metadata, ::std::collections::HashMap::new())
    }

    pub fn get_metadata(&self) -> &::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &self.metadata
    }

    fn get_metadata_for_reflect(&self) -> &::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &self.metadata
    }

    fn mut_metadata_for_reflect(&mut self) -> &mut ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &mut self.metadata
    }

    // optional int64 timestamp_ms = 6;

    pub fn clear_timestamp_ms(&mut self) {
        self.timestamp_ms = ::std::option::Option::None;
    }

    pub fn has_timestamp_ms(&self) -> bool {
        self.timestamp_ms.is_some()
    }

    // Param is passed by value, moved
    pub fn set_timestamp_ms(&mut self, v: i64) {
        self.timestamp_ms = ::std::option::Option::Some(v);
    }

    pub fn get_timestamp_ms(&self) -> i64 {
        self.timestamp_ms.unwrap_or(0)
    }

    fn get_timestamp_ms_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.timestamp_ms
    }

    fn mut_timestamp_ms_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.timestamp_ms
    }

    // repeated double bin_bounds = 7;

    pub fn clear_bin_bounds(&mut self) {
        self.bin_bounds.clear();
    }

    // Param is passed by value, moved
    pub fn set_bin_bounds(&mut self, v: ::std::vec::Vec<f64>) {
        self.bin_bounds = v;
    }

    // Mutable pointer to the field.
    pub fn mut_bin_bounds(&mut self) -> &mut ::std::vec::Vec<f64> {
        &mut self.bin_bounds
    }

    // Take field
    pub fn take_bin_bounds(&mut self) -> ::std::vec::Vec<f64> {
        ::std::mem::replace(&mut self.bin_bounds, ::std::vec::Vec::new())
    }

    pub fn get_bin_bounds(&self) -> &[f64] {
        &self.bin_bounds
    }

    fn get_bin_bounds_for_reflect(&self) -> &::std::vec::Vec<f64> {
        &self.bin_bounds
    }

    fn mut_bin_bounds_for_reflect(&mut self) -> &mut ::std::vec::Vec<f64> {
        &mut self.bin_bounds
    }
}

impl ::protobuf::Message for Telemetry {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.name)?;
                },
                2 => {
                    ::protobuf::rt::read_repeated_double_into(wire_type, is, &mut self.samples)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.persisted = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.method = ::std::option::Option::Some(tmp);
                },
                5 => {
                    ::protobuf::rt::read_map_into::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(wire_type, is, &mut self.metadata)?;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.timestamp_ms = ::std::option::Option::Some(tmp);
                },
                7 => {
                    ::protobuf::rt::read_repeated_double_into(wire_type, is, &mut self.bin_bounds)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.name.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if !self.samples.is_empty() {
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(self.samples.len() as u32) + (self.samples.len() * 8) as u32;
        }
        if let Some(v) = self.persisted {
            my_size += 2;
        }
        if let Some(v) = self.method {
            my_size += ::protobuf::rt::enum_size(4, v);
        }
        my_size += ::protobuf::rt::compute_map_size::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(5, &self.metadata);
        if let Some(v) = self.timestamp_ms {
            my_size += ::protobuf::rt::value_size(6, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += 9 * self.bin_bounds.len() as u32;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.name.as_ref() {
            os.write_string(1, &v)?;
        }
        if !self.samples.is_empty() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            // TODO: Data size is computed again, it should be cached
            os.write_raw_varint32((self.samples.len() * 8) as u32)?;
            for v in &self.samples {
                os.write_double_no_tag(*v)?;
            };
        }
        if let Some(v) = self.persisted {
            os.write_bool(3, v)?;
        }
        if let Some(v) = self.method {
            os.write_enum(4, v.value())?;
        }
        ::protobuf::rt::write_map_with_cached_sizes::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(5, &self.metadata, os)?;
        if let Some(v) = self.timestamp_ms {
            os.write_int64(6, v)?;
        }
        for v in &self.bin_bounds {
            os.write_double(7, *v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Telemetry {
    fn new() -> Telemetry {
        Telemetry::new()
    }

    fn descriptor_static(_: ::std::option::Option<Telemetry>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "name",
                    Telemetry::get_name_for_reflect,
                    Telemetry::mut_name_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeDouble>(
                    "samples",
                    Telemetry::get_samples_for_reflect,
                    Telemetry::mut_samples_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "persisted",
                    Telemetry::get_persisted_for_reflect,
                    Telemetry::mut_persisted_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<AggregationMethod>>(
                    "method",
                    Telemetry::get_method_for_reflect,
                    Telemetry::mut_method_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_map_accessor::<_, ::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(
                    "metadata",
                    Telemetry::get_metadata_for_reflect,
                    Telemetry::mut_metadata_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "timestamp_ms",
                    Telemetry::get_timestamp_ms_for_reflect,
                    Telemetry::mut_timestamp_ms_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeDouble>(
                    "bin_bounds",
                    Telemetry::get_bin_bounds_for_reflect,
                    Telemetry::mut_bin_bounds_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Telemetry>(
                    "Telemetry",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Telemetry {
    fn clear(&mut self) {
        self.clear_name();
        self.clear_samples();
        self.clear_persisted();
        self.clear_method();
        self.clear_metadata();
        self.clear_timestamp_ms();
        self.clear_bin_bounds();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Telemetry {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum AggregationMethod {
    SUM = 1,
    SET = 2,
    SUMMARIZE = 3,
    BIN = 4,
}

impl ::protobuf::ProtobufEnum for AggregationMethod {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<AggregationMethod> {
        match value {
            1 => ::std::option::Option::Some(AggregationMethod::SUM),
            2 => ::std::option::Option::Some(AggregationMethod::SET),
            3 => ::std::option::Option::Some(AggregationMethod::SUMMARIZE),
            4 => ::std::option::Option::Some(AggregationMethod::BIN),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [AggregationMethod] = &[
            AggregationMethod::SUM,
            AggregationMethod::SET,
            AggregationMethod::SUMMARIZE,
            AggregationMethod::BIN,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<AggregationMethod>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("AggregationMethod", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for AggregationMethod {
}

impl ::protobuf::reflect::ProtobufValue for AggregationMethod {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x20resources/protobufs/native.proto\x12\x14com.postmates.cernan\"w\n\
    \x07Payload\x127\n\x06points\x18\x02\x20\x03(\x0b2\x1f.com.postmates.cer\
    nan.TelemetryR\x06points\x123\n\x05lines\x18\x03\x20\x03(\x0b2\x1d.com.p\
    ostmates.cernan.LogLineR\x05lines\"\xdc\x01\n\x07LogLine\x12\x12\n\x04pa\
    th\x18\x01\x20\x01(\tR\x04path\x12\x14\n\x05value\x18\x02\x20\x01(\tR\
    \x05value\x12G\n\x08metadata\x18\x03\x20\x03(\x0b2+.com.postmates.cernan\
    .LogLine.MetadataEntryR\x08metadata\x12!\n\x0ctimestamp_ms\x18\x04\x20\
    \x01(\x03R\x0btimestampMs\x1a;\n\rMetadataEntry\x12\x10\n\x03key\x18\x01\
    \x20\x01(\tR\x03key\x12\x14\n\x05value\x18\x02\x20\x01(\tR\x05value:\x02\
    8\x01\"\xf8\x02\n\tTelemetry\x12\x12\n\x04name\x18\x01\x20\x01(\tR\x04na\
    me\x12\x1c\n\x07samples\x18\x02\x20\x03(\x01R\x07samplesB\x02\x10\x01\
    \x12#\n\tpersisted\x18\x03\x20\x01(\x08:\x05falseR\tpersisted\x12J\n\x06\
    method\x18\x04\x20\x01(\x0e2'.com.postmates.cernan.AggregationMethod:\tS\
    UMMARIZER\x06method\x12I\n\x08metadata\x18\x05\x20\x03(\x0b2-.com.postma\
    tes.cernan.Telemetry.MetadataEntryR\x08metadata\x12!\n\x0ctimestamp_ms\
    \x18\x06\x20\x01(\x03R\x0btimestampMs\x12\x1d\n\nbin_bounds\x18\x07\x20\
    \x03(\x01R\tbinBounds\x1a;\n\rMetadataEntry\x12\x10\n\x03key\x18\x01\x20\
    \x01(\tR\x03key\x12\x14\n\x05value\x18\x02\x20\x01(\tR\x05value:\x028\
    \x01*=\n\x11AggregationMethod\x12\x07\n\x03SUM\x10\x01\x12\x07\n\x03SET\
    \x10\x02\x12\r\n\tSUMMARIZE\x10\x03\x12\x07\n\x03BIN\x10\x04B\x16\n\x14c\
    om.postmates.cernanJ\xae(\n\x06\x12\x04\x20\0c\x01\n\xa0\x0c\n\x01\x0c\
    \x12\x03\x20\0\x12\x1a\xe3\x03\n\x20Welcome!\n\n\x20This\x20file\x20defi\
    nes\x20the\x20protocol\x20that\x20cernan\x20speaks\x20natively.\x20We\
    \x20hope\x20that\x20it's\n\x20a\x20relatively\x20straightforward\x20prot\
    ocol\x20to\x20implement.\x20Cernan's\x20native\x20transport\n\x20is\x20T\
    CP.\x20We\x20require\x20that\x20all\x20on-wire\x20payloads\x20have\x20th\
    e\x20following\x20form:\n\n\x20\x20\x20\x20\x20[------------------------\
    --------|~~~~~~~~~~\x20.\x20.\x20.\x20~~~~~~~~~~~~]\n\x20\x20\x20\x20\
    \x20^\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\
    \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20^\n\x20\
    \x20\x20\x20\x20u32,\x20payload\x20length\x20in\x20bytes\x20\x20\x20\x20\
    \x20protobuf\x20payload,\x20of\x20prefix\x20len\n\n\x20The\x20protobuf\
    \x20payload\x20conforms\x20to\x20the\x20following\x20definition.\n2\xaf\
    \x08\x20Copyright\x202016,\x20Postmates\x20Inc.\n\n\x20Permission\x20is\
    \x20hereby\x20granted,\x20free\x20of\x20charge,\x20to\x20any\x20person\
    \x20obtaining\x20a\x20copy\n\x20of\x20this\x20software\x20and\x20associa\
    ted\x20documentation\x20files\x20(the\x20\"Software\"),\x20to\x20deal\n\
    \x20in\x20the\x20Software\x20without\x20restriction,\x20including\x20wit\
    hout\x20limitation\x20the\x20rights\n\x20to\x20use,\x20copy,\x20modify,\
    \x20merge,\x20publish,\x20distribute,\x20sublicense,\x20and/or\x20sell\n\
    \x20copies\x20of\x20the\x20Software,\x20and\x20to\x20permit\x20persons\
    \x20to\x20whom\x20the\x20Software\x20is\n\x20furnished\x20to\x20do\x20so\
    ,\x20subject\x20to\x20the\x20following\x20conditions:\n\n\x20The\x20abov\
    e\x20copyright\x20notice\x20and\x20this\x20permission\x20notice\x20shall\
    \x20be\x20included\x20in\n\x20all\x20copies\x20or\x20substantial\x20port\
    ions\x20of\x20the\x20Software.\n\n\x20THE\x20SOFTWARE\x20IS\x20PROVIDED\
    \x20\"AS\x20IS\",\x20WITHOUT\x20WARRANTY\x20OF\x20ANY\x20KIND,\x20EXPRES\
    S\x20OR\n\x20IMPLIED,\x20INCLUDING\x20BUT\x20NOT\x20LIMITED\x20TO\x20THE\
    \x20WARRANTIES\x20OF\x20MERCHANTABILITY,\n\x20FITNESS\x20FOR\x20A\x20PAR\
    TICULAR\x20PURPOSE\x20AND\x20NONINFRINGEMENT.\x20IN\x20NO\x20EVENT\x20SH\
    ALL\x20THE\n\x20AUTHORS\x20OR\x20COPYRIGHT\x20HOLDERS\x20BE\x20LIABLE\
    \x20FOR\x20ANY\x20CLAIM,\x20DAMAGES\x20OR\x20OTHER\n\x20LIABILITY,\x20WH\
    ETHER\x20IN\x20AN\x20ACTION\x20OF\x20CONTRACT,\x20TORT\x20OR\x20OTHERWIS\
    E,\x20ARISING\x20FROM,\n\x20OUT\x20OF\x20OR\x20IN\x20CONNECTION\x20WITH\
    \x20THE\x20SOFTWARE\x20OR\x20THE\x20USE\x20OR\x20OTHER\x20DEALINGS\x20IN\
    \x20THE\n\x20SOFTWARE.\n\n\x08\n\x01\x02\x12\x03\"\x08\x1c\n\x08\n\x01\
    \x08\x12\x03#\0-\n\x0b\n\x04\x08\xe7\x07\0\x12\x03#\0-\n\x0c\n\x05\x08\
    \xe7\x07\0\x02\x12\x03#\x07\x13\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03#\
    \x07\x13\n\x0e\n\x07\x08\xe7\x07\0\x02\0\x01\x12\x03#\x07\x13\n\x0c\n\
    \x05\x08\xe7\x07\0\x07\x12\x03#\x16,\n\xb0\x01\n\x02\x04\0\x12\x04)\0,\
    \x01\x1a\xa3\x01\x20'Payload'\x20-\x20the\x20top-level\x20structure\x20i\
    n\x20each\x20on-wire\x20payload\n\n\x20Payload\x20is\x20a\x20container\
    \x20for\x20repeated\x20Telemetry\x20and\x20LogLines.\x20There's\x20not\
    \x20much\n\x20more\x20to\x20it\x20than\x20that.\n\n\n\n\x03\x04\0\x01\
    \x12\x03)\x08\x0f\n\x0b\n\x04\x04\0\x02\0\x12\x03*\x02\x20\n\x0c\n\x05\
    \x04\0\x02\0\x04\x12\x03*\x02\n\n\x0c\n\x05\x04\0\x02\0\x06\x12\x03*\x0b\
    \x14\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03*\x15\x1b\n\x0c\n\x05\x04\0\x02\
    \0\x03\x12\x03*\x1e\x1f\n\x0b\n\x04\x04\0\x02\x01\x12\x03+\x02\x1d\n\x0c\
    \n\x05\x04\0\x02\x01\x04\x12\x03+\x02\n\n\x0c\n\x05\x04\0\x02\x01\x06\
    \x12\x03+\x0b\x12\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03+\x13\x18\n\x0c\n\
    \x05\x04\0\x02\x01\x03\x12\x03+\x1b\x1c\n\x99\x02\n\x02\x04\x01\x12\x044\
    \09\x01\x1a\x8c\x02\x20'LogLine'\x20-\x20a\x20bit\x20of\x20unstructure\
    \x20text\n\n\x20One\x20of\x20cernan's\x20gigs\x20is\x20picking\x20up\x20\
    logs\x20from\x20disk\x20and\x20transforming\x20them\n\x20in-flight,\x20s\
    hipping\x20them\x20off.\x20This\x20structure\x20allows\x20you\x20to\x20s\
    hip\x20lines\n\x20directly\x20via\x20the\x20native\x20protocol\x20withou\
    t\x20having\x20to\x20round-trip\x20through\x20disk\n\x20first.\n\n\n\n\
    \x03\x04\x01\x01\x12\x034\x08\x0f\n0\n\x04\x04\x01\x02\0\x12\x035\x02\
    \x1b\"#\x20unique\x20'location'\x20of\x20the\x20log\x20line\n\n\x0c\n\
    \x05\x04\x01\x02\0\x04\x12\x035\x02\n\n\x0c\n\x05\x04\x01\x02\0\x05\x12\
    \x035\x0b\x11\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x035\x12\x16\n\x0c\n\x05\
    \x04\x01\x02\0\x03\x12\x035\x19\x1a\n\x1e\n\x04\x04\x01\x02\x01\x12\x036\
    \x02\x1c\"\x11\x20the\x20line\x20itself\n\n\x0c\n\x05\x04\x01\x02\x01\
    \x04\x12\x036\x02\n\n\x0c\n\x05\x04\x01\x02\x01\x05\x12\x036\x0b\x11\n\
    \x0c\n\x05\x04\x01\x02\x01\x01\x12\x036\x12\x17\n\x0c\n\x05\x04\x01\x02\
    \x01\x03\x12\x036\x1a\x1b\n,\n\x04\x04\x01\x02\x02\x12\x037\x02#\"\x1f\
    \x20associated\x20key/value\x20metadata\n\n\r\n\x05\x04\x01\x02\x02\x04\
    \x12\x047\x026\x1c\n\x0c\n\x05\x04\x01\x02\x02\x06\x12\x037\x02\x15\n\
    \x0c\n\x05\x04\x01\x02\x02\x01\x12\x037\x16\x1e\n\x0c\n\x05\x04\x01\x02\
    \x02\x03\x12\x037!\"\n0\n\x04\x04\x01\x02\x03\x12\x038\x02\"\"#\x20milli\
    seconds\x20since\x20the\x20Unix\x20epoch\n\n\x0c\n\x05\x04\x01\x02\x03\
    \x04\x12\x038\x02\n\n\x0c\n\x05\x04\x01\x02\x03\x05\x12\x038\x0b\x10\n\
    \x0c\n\x05\x04\x01\x02\x03\x01\x12\x038\x11\x1d\n\x0c\n\x05\x04\x01\x02\
    \x03\x03\x12\x038\x20!\n\x8f\x04\n\x02\x04\x02\x12\x04D\0L\x01\x1a\x82\
    \x04\x20'Telemetry'\x20-\x20a\x20numeric\x20measure\x20of\x20a\x20thing\
    \n\n\x20Cernan's\x20slightly\x20more\x20complicated\x20gig\x20is\x20its\
    \x20'telemetry'\n\x20subsystem.\x20Telemetry\x20is\x20defined\x20as\x20a\
    \x20name\x20and\x20time\x20associated\x20collection\x20of\n\x20measureme\
    nts.\x20In\x20the\x20structure\x20we\x20refer\x20to\x20these\x20measurem\
    ents\x20as\n\x20'samples'.\x20The\x20Telemetry\x20structure\x20makes\x20\
    is\x20possible\x20to\x20associate\x20multiple\n\x20samples\x20in\x20a\
    \x20single\x20millisecond\x20time\x20window.\x20Cernan\x20will\x20build\
    \x20a\x20quantile\n\x20structure\x20over\x20these\x20samples\x20but\x20y\
    ou\x20may\x20further\x20choose\x20aggregation\n\x20interpretations\x20by\
    \x20setting\x20AggregationMethod.\n\n\n\n\x03\x04\x02\x01\x12\x03D\x08\
    \x11\n/\n\x04\x04\x02\x02\0\x12\x03E\x02\x1b\"\"\x20the\x20unique\x20nam\
    e\x20of\x20the\x20telemetry\n\n\x0c\n\x05\x04\x02\x02\0\x04\x12\x03E\x02\
    \n\n\x0c\n\x05\x04\x02\x02\0\x05\x12\x03E\x0b\x11\n\x0c\n\x05\x04\x02\
    \x02\0\x01\x12\x03E\x12\x16\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03E\x19\
    \x1a\n8\n\x04\x04\x02\x02\x01\x12\x03F\x020\"+\x20telemetry\x20samples\
    \x20present\x20in\x20timestamp_ms\n\n\x0c\n\x05\x04\x02\x02\x01\x04\x12\
    \x03F\x02\n\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03F\x0b\x11\n\x0c\n\x05\
    \x04\x02\x02\x01\x01\x12\x03F\x12\x19\n\x0c\n\x05\x04\x02\x02\x01\x03\
    \x12\x03F\x1c\x1d\n\x0c\n\x05\x04\x02\x02\x01\x08\x12\x03F\x1e/\n\x0f\n\
    \x08\x04\x02\x02\x01\x08\xe7\x07\0\x12\x03F\x20-\n\x10\n\t\x04\x02\x02\
    \x01\x08\xe7\x07\0\x02\x12\x03F\x20&\n\x11\n\n\x04\x02\x02\x01\x08\xe7\
    \x07\0\x02\0\x12\x03F\x20&\n\x12\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\x02\
    \0\x01\x12\x03F\x20&\n\x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\x12\x03\
    F)-\n1\n\x04\x04\x02\x02\x02\x12\x03G\x022\"$\x20persist\x20metric\x20ac\
    ross\x20time\x20windows\n\n\x0c\n\x05\x04\x02\x02\x02\x04\x12\x03G\x02\n\
    \n\x0c\n\x05\x04\x02\x02\x02\x05\x12\x03G\x0b\x0f\n\x0c\n\x05\x04\x02\
    \x02\x02\x01\x12\x03G\x10\x19\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x03G\
    \x1c\x1d\n\x0c\n\x05\x04\x02\x02\x02\x08\x12\x03G\x1e1\n\x0c\n\x05\x04\
    \x02\x02\x02\x07\x12\x03G*/\n\x18\n\x04\x04\x02\x02\x03\x12\x03H\x02@\"\
    \x0b\x20see\x20below\n\n\x0c\n\x05\x04\x02\x02\x03\x04\x12\x03H\x02\n\n\
    \x0c\n\x05\x04\x02\x02\x03\x06\x12\x03H\x0b\x1c\n\x0c\n\x05\x04\x02\x02\
    \x03\x01\x12\x03H\x1d#\n\x0c\n\x05\x04\x02\x02\x03\x03\x12\x03H&'\n\x0c\
    \n\x05\x04\x02\x02\x03\x08\x12\x03H(?\n\x0c\n\x05\x04\x02\x02\x03\x07\
    \x12\x03H4=\n,\n\x04\x04\x02\x02\x04\x12\x03I\x02#\"\x1f\x20associated\
    \x20key/value\x20metadata\n\n\r\n\x05\x04\x02\x02\x04\x04\x12\x04I\x02H@\
    \n\x0c\n\x05\x04\x02\x02\x04\x06\x12\x03I\x02\x15\n\x0c\n\x05\x04\x02\
    \x02\x04\x01\x12\x03I\x16\x1e\n\x0c\n\x05\x04\x02\x02\x04\x03\x12\x03I!\
    \"\n0\n\x04\x04\x02\x02\x05\x12\x03J\x02\"\"#\x20milliseconds\x20since\
    \x20the\x20Unix\x20epoch\n\n\x0c\n\x05\x04\x02\x02\x05\x04\x12\x03J\x02\
    \n\n\x0c\n\x05\x04\x02\x02\x05\x05\x12\x03J\x0b\x10\n\x0c\n\x05\x04\x02\
    \x02\x05\x01\x12\x03J\x11\x1d\n\x0c\n\x05\x04\x02\x02\x05\x03\x12\x03J\
    \x20!\n)\n\x04\x04\x02\x02\x06\x12\x03K\x02!\"\x1c\x20BIN\x20inclusive\
    \x20upper\x20bounds\n\n\x0c\n\x05\x04\x02\x02\x06\x04\x12\x03K\x02\n\n\
    \x0c\n\x05\x04\x02\x02\x06\x05\x12\x03K\x0b\x11\n\x0c\n\x05\x04\x02\x02\
    \x06\x01\x12\x03K\x12\x1c\n\x0c\n\x05\x04\x02\x02\x06\x03\x12\x03K\x1f\
    \x20\n\xde\x03\n\x02\x05\0\x12\x04V\0c\x01\x1a\xd1\x03\x20'AggregationMe\
    thod'\x20-\x20an\x20interpretation\x20signal\n\n\x20Cernan\x20maintains\
    \x20quantile\x20summaries\x20for\x20all\x20Telemetry\x20samples.\x20Not\
    \x20all\x20sinks\n\x20are\x20capable\x20of\x20interpreting\x20summaries\
    \x20natively.\x20Cernan\x20allows\x20the\x20client\x20to\n\x20set\x20pre\
    ferred\x20aggregations\x20over\x20the\x20summaries\x20for\x20reporting\
    \x20to\x20'flat'\n\x20sinks.\x20Sinks\x20are\x20allows\x20to\x20ignore\
    \x20AggregationMethod\x20at\x20their\n\x20convenience.\x20Additionally,\
    \x20aggregation\x20time\x20windows\x20may\x20be\x20configured\n\x20per-s\
    ink\x20and\x20are\x20not\x20controllable\x20through\x20the\x20protocol.\
    \n\n\n\n\x03\x05\0\x01\x12\x03V\x05\x16\n^\n\x04\x05\0\x02\0\x12\x03Y\
    \x02\n\x1aQ\x20SUM\x20keeps\x20a\x20sum\x20of\x20samples.\x20This\x20is\
    \x20often\x20interpreted\x20as\x20a\n\x20per-window\x20counter.\n\n\x0c\
    \n\x05\x05\0\x02\0\x01\x12\x03Y\x02\x05\n\x0c\n\x05\x05\0\x02\0\x02\x12\
    \x03Y\x08\t\nU\n\x04\x05\0\x02\x01\x12\x03\\\x02\n\x1aH\x20SET\x20preser\
    ves\x20the\x20last\x20sample\x20set\x20into\x20the\x20Telemetry\x20per\
    \x20time\n\x20window.\n\n\x0c\n\x05\x05\0\x02\x01\x01\x12\x03\\\x02\x05\
    \n\x0c\n\x05\x05\0\x02\x01\x02\x12\x03\\\x08\t\nz\n\x04\x05\0\x02\x02\
    \x12\x03_\x02\x10\x1am\x20SUMMARIZE\x20produces\x20a\x20quantile\x20summ\
    ary\x20of\x20the\x20input\x20samples\x20per\x20time\n\x20window.\x20This\
    \x20is\x20the\x20default\x20behaviour.\n\n\x0c\n\x05\x05\0\x02\x02\x01\
    \x12\x03_\x02\x0b\n\x0c\n\x05\x05\0\x02\x02\x02\x12\x03_\x0e\x0f\n\x84\
    \x01\n\x04\x05\0\x02\x03\x12\x03b\x02\n\x1aw\x20BIN\x20produces\x20a\x20\
    histogram\x20summary\x20of\x20the\x20input\x20samples\x20per\x20time\x20\
    window.\x20The\n\x20user\x20will\x20specify\x20the\x20bins'\x20upper\x20\
    bounds.\n\n\x0c\n\x05\x05\0\x02\x03\x01\x12\x03b\x02\x05\n\x0c\n\x05\x05\
    \0\x02\x03\x02\x12\x03b\x08\t\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
