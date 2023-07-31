/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include "seastarx.hh"

namespace tools::utils {

class basic_option {
public:
    const char* name;
    const char* description;

public:
    basic_option(const char* name, const char* description) : name(name), description(description) { }

    virtual void add_option(boost::program_options::options_description& opts) const = 0;
};

template <typename T = std::monostate>
class typed_option : public basic_option {
    std::optional<T> _default_value;
    int _count;

    virtual void add_option(boost::program_options::options_description& opts) const override {
        if (_default_value) {
            opts.add_options()(name, boost::program_options::value<T>()->default_value(*_default_value), description);
        } else {
            opts.add_options()(name, boost::program_options::value<T>(), description);
        }
    }

public:
    typed_option(const char* name, const char* description) : basic_option(name, description) { }
    typed_option(const char* name, T default_value, const char* description) : basic_option(name, description), _default_value(std::move(default_value)) { }
    typed_option(const char* name, const char* description, int count) : basic_option(name, description), _count(count) { }
};

template <>
class typed_option<std::monostate> : public basic_option {
    virtual void add_option(boost::program_options::options_description& opts) const override {
        opts.add_options()(name, description);
    }
public:
    typed_option(const char* name, const char* description) : basic_option(name, description) { }
};

class operation_option {
    shared_ptr<basic_option> _opt; // need copy to support convenient range declaration of std::vector<option>

public:
    template <typename T>
    operation_option(typed_option<T> opt) : _opt(make_shared<typed_option<T>>(std::move(opt))) { }

    const char* name() const { return _opt->name; }
    const char* description() const { return _opt->description; }
    void add_option(boost::program_options::options_description& opts) const { _opt->add_option(opts); }
};

class operation {
    std::string _name;
    std::string _summary;
    std::string _description;
    std::vector<operation_option> _options;
    std::vector<app_template::positional_option> _positional_options;

public:
    operation(
            std::string name,
            std::string summary,
            std::string description,
            std::vector<operation_option> options = {},
            std::vector<app_template::positional_option> positional_options = {})
        : _name(std::move(name))
        , _summary(std::move(summary))
        , _description(std::move(description))
        , _options(std::move(options))
        , _positional_options(std::move(positional_options)) {
    }

    const std::string& name() const { return _name; }
    const std::string& summary() const { return _summary; }
    const std::string& description() const { return _description; }
    const std::vector<operation_option>& options() const { return _options; }
    const std::vector<app_template::positional_option>& positional_options() const { return _positional_options; }
};

inline bool operator<(const operation& a, const operation& b) {
    return a.name() < b.name();
}

class tool_app_template {
public:
    struct config {
        sstring name;
        sstring description;
        sstring logger_name;
        size_t lsa_segment_pool_backend_size_mb = 1;
        std::vector<operation> operations;
        const std::vector<operation_option>* global_options = nullptr;
        const std::vector<app_template::positional_option>* global_positional_options = nullptr;
    };

private:
    config _cfg;

public:
    tool_app_template(config cfg)
        : _cfg(std::move(cfg))
    { }

    int run_async(int argc, char** argv, noncopyable_function<int(const operation&, const boost::program_options::variables_map&)> main_func);
};

} // namespace tools::utils
