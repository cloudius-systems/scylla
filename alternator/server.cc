/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "alternator/server.hh"
#include "log.hh"
#include <seastar/http/function_handlers.hh>
#include <seastar/json/json_elements.hh>
#include <seastarx.hh>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "error.hh"
#include "rjson.hh"

static logging::logger slogger("alternator-server");

using namespace httpd;

namespace alternator {

static constexpr auto TARGET = "X-Amz-Target";

inline std::vector<sstring> split(const sstring& text, const char* separator) {
    if (text == "") {
        return std::vector<sstring>();
    }
    std::vector<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}

// DynamoDB HTTP error responses are structured as follows
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// Our handlers throw an exception to report an error. If the exception
// is of type alternator::api_error, it unwrapped and properly reported to
// the user directly. Other exceptions are unexpected, and reported as
// Internal Server Error.
class api_handler : public handler_base {
public:
    api_handler(const future_json_function& _handle) : _f_handle(
         [_handle](std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
         return seastar::futurize_apply(_handle, std::move(req)).then_wrapped([rep = std::move(rep)](future<json::json_return_type> resf) mutable {
             if (resf.failed()) {
                 // Exceptions of type api_error are wrapped as JSON and
                 // returned to the client as expected. Other types of
                 // exceptions are unexpected, and returned to the user
                 // as an internal server error:
                 api_error ret;
                 try {
                     resf.get();
                 } catch (api_error &ae) {
                     ret = ae;
                 } catch (rjson::error & re) {
                     ret = api_error("ValidationException", re.what());
                 } catch (...) {
                     ret = api_error(
                             "Internal Server Error",
                             format("Internal server error: {}", std::current_exception()),
                             reply::status_type::internal_server_error);
                 }
                 // FIXME: what is this version number?
                 rep->_content += "{\"__type\":\"com.amazonaws.dynamodb.v20120810#" + ret._type + "\"," +
                         "\"message\":\"" + ret._msg + "\"}";
                 rep->_status = ret._http_code;
                 slogger.trace("api_handler error case: {}", rep->_content);
                 return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
             }
             slogger.trace("api_handler success case");
             auto res = resf.get0();
             if (res._body_writer) {
                 rep->write_body("json", std::move(res._body_writer));
             } else {
                 rep->_content += res._res;
             }
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
         });
    }), _type("json") { }

    api_handler(const api_handler&) = default;
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        return _f_handle(std::move(req), std::move(rep)).then(
                [this](std::unique_ptr<reply> rep) {
                    rep->done(_type);
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

protected:
    future_handler_function _f_handle;
    sstring _type;
};

void server::set_routes(routes& r) {
    using alternator_callback = std::function<future<json::json_return_type>(executor&, std::unique_ptr<request>)>;
    std::unordered_map<std::string, alternator_callback> routes{
        {"CreateTable", [] (executor& e, std::unique_ptr<request> req) { return e.create_table(req->content); }},
        {"DescribeTable", [] (executor& e, std::unique_ptr<request> req) { return e.describe_table(req->content); }},
        {"DeleteTable", [] (executor& e, std::unique_ptr<request> req) { return e.delete_table(req->content); }},
        {"PutItem", [] (executor& e, std::unique_ptr<request> req) { return e.put_item(req->content); }},
        {"UpdateItem", [] (executor& e, std::unique_ptr<request> req) { return e.update_item(req->content); }},
        {"GetItem", [] (executor& e, std::unique_ptr<request> req) { return e.get_item(req->content); }},
        {"DeleteItem", [] (executor& e, std::unique_ptr<request> req) { return e.delete_item(req->content); }},
        {"ListTables", [] (executor& e, std::unique_ptr<request> req) { return e.list_tables(req->content); }},
        {"Scan", [] (executor& e, std::unique_ptr<request> req) { return e.scan(req->content); }},
        {"DescribeEndpoints", [] (executor& e, std::unique_ptr<request> req) { return e.describe_endpoints(req->content, req->get_header("Host")); }},
        {"BatchWriteItem", [] (executor& e, std::unique_ptr<request> req) { return e.batch_write_item(req->content); }},
        {"BatchGetItem", [] (executor& e, std::unique_ptr<request> req) { return e.batch_get_item(req->content); }},
        {"Query", [] (executor& e, std::unique_ptr<request> req) { return e.query(req->content); }},
    };

    api_handler* handler = new api_handler([this, routes = std::move(routes)](std::unique_ptr<request> req) -> future<json::json_return_type> {
        _executor.local()._stats.total_operations++;
        slogger.trace("Raw request: {} ({})", req->content, req->content_length);
        sstring target = req->get_header(TARGET);
        std::vector<sstring> split_target = split(target, ".");
        //NOTICE(sarna): Target consists of Dynamo API version folllowed by a dot '.' and operation type (e.g. CreateTable)
        sstring op = split_target.empty() ? sstring() : split_target.back();

        slogger.trace("Request type: {}", op);
        auto callback_it = routes.find(op);
        if (callback_it == routes.end()) {
            _executor.local()._stats.unsupported_operations++;
            throw api_error("UnknownOperationException",
                    format("Unsupported operation {}", op));
        }
        return callback_it->second(_executor.local(), std::move(req));
    });

    r.add(operation_type::POST, url("/"), handler);
}

future<> server::init(uint16_t port) {
    return _executor.invoke_on_all([] (executor& e) {
        return e.start();
    }).then([this] {
        return _control.start();
    }).then([this] {
        return _control.set_routes(std::bind(&server::set_routes, this, std::placeholders::_1));
    }).then([this, port] {
        return _control.listen(port);
    }).then([port] {
        slogger.info("Alternator HTTP server listening on port {}", port);
    }).handle_exception([port] (std::exception_ptr e) {
        slogger.warn("Failed to set up Alternator HTTP server on port {}: {}", port, e);
    });
}

}

