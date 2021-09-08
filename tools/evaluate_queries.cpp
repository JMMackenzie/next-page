#include <iostream>
#include <optional>
#include <thread>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <functional>
#include <mappable/mapper.hpp>
#include <mio/mmap.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>

#include "accumulator/lazy_accumulator.hpp"
#include "app.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::views::enumerate;

template <typename IndexType, typename WandType>
void evaluate_queries(
    const std::string& index_filename,
    const std::string& wand_data_filename,
    const std::vector<Query>& queries,
    const std::optional<std::string>& thresholds_filename,
    std::string const& type,
    std::string const& query_type,
    uint64_t k,
    uint64_t secondary_k,
    std::string const& documents_filename,
    ScorerParams const& scorer_params,
    std::string const& run_id,
    std::string const& iteration)
{
    IndexType index(MemorySource::mapped_file(index_filename));
    WandType const wdata(MemorySource::mapped_file(wand_data_filename));

    auto scorer = scorer::from_params(scorer_params, wdata);
    std::function<std::tuple<
        std::vector<std::pair<float, uint64_t>>,
        std::vector<std::pair<float, uint64_t>>>(Query)> query_fun;

    if (query_type == "wand") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(0);
            cyclic_queue cyclic(0);
            wand_query wand_q(topk, secondary, cyclic);
            wand_q(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return std::make_tuple(topk.topk(), secondary.topk());
        };
    } else if (query_type == "wand_method_1") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(secondary_k);
            cyclic_queue cyclic(secondary_k);
            wand_query wand_q(topk, secondary, cyclic);
            wand_q.method_one(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            cyclic.finalize();
            return std::make_tuple(topk.topk(), cyclic.topk());
        };
    } else if (query_type == "wand_method_2") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(secondary_k);
            cyclic_queue cyclic(secondary_k);
            wand_query wand_q(topk, secondary, cyclic);
            wand_q.method_two(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            secondary.finalize();
            return std::make_tuple(topk.topk(), secondary.topk());
        };
     } else if (query_type == "wand_method_3") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(secondary_k);
            cyclic_queue cyclic(secondary_k);
            wand_query wand_q(topk, secondary, cyclic);
            wand_q.method_three(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            secondary.finalize();
            return std::make_tuple(topk.topk(), secondary.topk());
        };
    } else if (query_type == "block_max_wand") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(0);
            cyclic_queue cyclic(0);
            block_max_wand_query block_max_wand_q(topk, secondary, cyclic);
            block_max_wand_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return std::make_tuple(topk.topk(), secondary.topk());
        };
    } else if (query_type == "block_max_wand_method_1") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(secondary_k);
            cyclic_queue cyclic(secondary_k);
            block_max_wand_query block_max_wand_q(topk, secondary, cyclic);
            block_max_wand_q.method_one(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            cyclic.finalize();
            return std::make_tuple(topk.topk(), cyclic.topk());
        };
     } else if (query_type == "block_max_wand_method_2") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(secondary_k);
            cyclic_queue cyclic(secondary_k);
            block_max_wand_query block_max_wand_q(topk, secondary, cyclic);
            block_max_wand_q.method_two(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            secondary.finalize();
            return std::make_tuple(topk.topk(), secondary.topk());
        };
      } else if (query_type == "block_max_wand_method_3") {
        query_fun = [&](Query query) {
            topk_queue topk(k);
            topk_queue secondary(secondary_k);
            cyclic_queue cyclic(secondary_k);
            block_max_wand_query block_max_wand_q(topk, secondary, cyclic);
            block_max_wand_q.method_three(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            secondary.finalize();
            return std::make_tuple(topk.topk(), secondary.topk());
        };
    } else {
        spdlog::error("Unsupported query type: {}", query_type);
    }

    auto source = std::make_shared<mio::mmap_source>(documents_filename.c_str());
    auto docmap = Payload_Vector<>::from(*source);

    std::vector<std::vector<std::pair<float, uint64_t>>> raw_results(queries.size());

    for (size_t query_idx = 0; query_idx < queries.size(); ++query_idx) {

        auto results = query_fun(queries[query_idx]);
        auto qid = queries[query_idx].id;
        size_t res_count = 0;
        for (auto&& [rank, result]: enumerate(std::get<0>(results))) {
            std::cout << fmt::format(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                qid.value_or(std::to_string(query_idx)),
                iteration,
                docmap[result.second],
                rank,
                result.first,
                run_id);
            ++res_count;
        }
        for (auto && [rank, result]: enumerate(std::get<1>(results))) {
            std::cout << fmt::format(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                qid.value_or(std::to_string(query_idx)),
                iteration,
                docmap[result.second],
                rank + res_count,
                result.first,
                run_id);
 
        }
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    spdlog::set_default_logger(spdlog::stderr_color_mt("default"));

    std::string documents_file;
    std::string run_id = "R0";
    bool quantized = false;
    uint64_t secondary_k = 0;

    App<arg::Index,
        arg::WandData<arg::WandMode::Required>,
        arg::Query<arg::QueryMode::Ranked>,
        arg::Algorithm,
        arg::Scorer,
        arg::Thresholds,
        arg::Threads>
        app{"Retrieves query results in TREC format."};
    app.add_option("-r,--run", run_id, "Run identifier");
    app.add_option("--documents", documents_file, "Document lexicon")->required();
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_option("--secondary-k", secondary_k, "Size of secondary heap/queue.")->required();
 
    CLI11_PARSE(app, argc, argv);

    tbb::global_control control(tbb::global_control::max_allowed_parallelism, app.threads() + 1);
    spdlog::info("Number of worker threads: {}", app.threads());

    if (run_id.empty()) {
        run_id = "PISA";
    }

    auto iteration = "Q0";

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        app.queries(),
        app.thresholds_file(),
        app.index_encoding(),
        app.algorithm(),
        app.k(),
        secondary_k,
        documents_file,
        app.scorer_params(),
        run_id,
        iteration);

    /**/
    if (false) {  // NOLINT
#define LOOP_BODY(R, DATA, T)                                                                      \
    }                                                                                              \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                        \
    {                                                                                              \
        if (app.is_wand_compressed()) {                                                            \
            if (quantized) {                                                                       \
                std::apply(                                                                        \
                    evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>,       \
                    params);                                                                       \
            } else {                                                                               \
                std::apply(evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index>, params); \
            }                                                                                      \
        } else {                                                                                   \
            std::apply(evaluate_queries<BOOST_PP_CAT(T, _index), wand_raw_index>, params);         \
        }                                                                                          \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
