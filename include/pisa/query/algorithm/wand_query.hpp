#pragma once

#include <vector>

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "cyclic_queue.hpp"

namespace pisa {

struct wand_query {
    explicit wand_query(topk_queue& topk, topk_queue& secondary, cyclic_queue& cyclic) : m_topk(topk), m_secondary(secondary), m_cyclic(cyclic) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        auto sort_enums = [&]() {
            // sort enumerators by increasing docid
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
                return lhs->docid() < rhs->docid();
            });
        };

        sort_enums();
        while (true) {
            // find pivot
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() >= max_docid) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_cursors[pivot]->docid();
            if (pivot_id == ordered_cursors[0]->docid()) {
                float score = 0;
                for (Cursor* en: ordered_cursors) {
                    if (en->docid() != pivot_id) {
                        break;
                    }
                    score += en->score();
                    en->next();
                }

                m_topk.insert(score, pivot_id);
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {
                }
                ordered_cursors[next_list]->next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() < ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    //NEXTPAGE: Method 1 is like regular WAND processing but it puts the ejected
    //heap documents into a secondary cyclic queue on the way through
    template <typename CursorRange>
    void method_one(CursorRange&& cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        auto sort_enums = [&]() {
            // sort enumerators by increasing docid
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
                return lhs->docid() < rhs->docid();
            });
        };

        sort_enums();
        while (true) {
            // find pivot
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() >= max_docid) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_cursors[pivot]->docid();
            if (pivot_id == ordered_cursors[0]->docid()) {
                float score = 0;
                for (Cursor* en: ordered_cursors) {
                    if (en->docid() != pivot_id) {
                        break;
                    }
                    score += en->score();
                    en->next();
                }

                uint64_t ejected_docid = 0;
                float ejected_score = 0.f;
                // If the pivot goes in, we store the ejected doc into the cyclic
                if(m_topk.insert(score, pivot_id, ejected_score, ejected_docid)) {
                    m_cyclic.insert(ejected_score, ejected_docid);
                }
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {
                }
                ordered_cursors[next_list]->next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() < ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    //NEXTPAGE: Method 2 is the same as Method 1 but it also collects the documents
    // which are scored but do not enter the heap
    template <typename CursorRange>
    void method_two(CursorRange&& cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        auto sort_enums = [&]() {
            // sort enumerators by increasing docid
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
                return lhs->docid() < rhs->docid();
            });
        };

        sort_enums();
        while (true) {
            // find pivot
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() >= max_docid) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_cursors[pivot]->docid();
            if (pivot_id == ordered_cursors[0]->docid()) {
                float score = 0;
                for (Cursor* en: ordered_cursors) {
                    if (en->docid() != pivot_id) {
                        break;
                    }
                    score += en->score();
                    en->next();
                }

                uint64_t ejected_docid = 0;
                float ejected_score = 0.f;
                // If the pivot goes in, we put the ejected document into the secondary
                // otherwise, we try to put the pivot in there
                if(m_topk.insert(score, pivot_id, ejected_score, ejected_docid)) {
                    m_secondary.insert(ejected_score, ejected_docid);
                } else {
                    m_secondary.insert(score, pivot_id);
                }
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {
                }
                ordered_cursors[next_list]->next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() < ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    //NEXTPAGE: Method 3 is a safe-to-k method; in the first pass, it keeps a bitvector
    // which tracks documents which have been scored already. It also uses the cyclic queue
    // to determine the first safe position to start processing during the second pass
    template <typename CursorRange>
    void method_three(CursorRange&& cursors, uint64_t max_docid)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        // Create an empty bitvector of max_docid elements
        // XXX: Do we need to init for each query, or can we
        // pass through from the constructor?
        bit_vector_builder scored(max_docid, false);

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        auto sort_enums = [&]() {
            // sort enumerators by increasing docid
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
                return lhs->docid() < rhs->docid();
            });
        };

        sort_enums();
        while (true) {
            // find pivot
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() >= max_docid) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_cursors[pivot]->docid();
            if (pivot_id == ordered_cursors[0]->docid()) {
                float score = 0;
                for (Cursor* en: ordered_cursors) {
                    if (en->docid() != pivot_id) {
                        break;
                    }
                    score += en->score();
                    en->next();
                }
                scored.set(pivot_id, true);

                uint64_t ejected_docid = 0;
                float ejected_score = 0.f;
                // If the pivot goes in, we put the ejected document into the secondary
                // otherwise, we try to put the pivot in there
                // we also track the threshold at the point of ejection using the cyclic
                if(m_topk.insert(score, pivot_id, ejected_score, ejected_docid)) {
                    m_secondary.insert(ejected_score, ejected_docid);
                    // when pivot_id was scored, ejected_score was the threshold
                    m_cyclic.insert(ejected_score, pivot_id); 
                } else {
                    m_secondary.insert(score, pivot_id);
                }
                // resort by docid
                sort_enums();
            } else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {
                }
                ordered_cursors[next_list]->next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() < ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }

        // Stage one completed; first page is safe
        // Find the lowest docid which might have been missed
        size_t lower_bound = m_cyclic.displaced_id(m_secondary.threshold());
        
        // Reset cursors on the lower bound
        for (auto &en : ordered_cursors) {
            en->reset();
            en->next_geq(lower_bound);
        }

        // Stage two: pick up remaining documents
        sort_enums();

        while (true) {
            // find pivot
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() >= max_docid) {
                    break;
                }
                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_secondary.would_enter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            // check if pivot is a possible match
            uint64_t pivot_id = ordered_cursors[pivot]->docid();

            // Case 1: We've scored this document. Move on.
            if (scored[pivot_id]) {
                ordered_cursors[pivot]->next();
                // Bubble down the advanced list
                for (size_t i = pivot + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() <= ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }

            // Case 2: We are yet to score it, and the pivots are aligned. So we score.
            if (pivot_id == ordered_cursors[0]->docid()) {
                float score = 0;
                for (Cursor* en: ordered_cursors) {
                    if (en->docid() != pivot_id) {
                        break;
                    }
                    score += en->score();
                    en->next();
                }
                m_secondary.insert(score, pivot_id);
                // resort by docid
                sort_enums();
            } 
            
            // Case 3: Pivots need aligning
            else {
                // no match, move farthest list up to the pivot
                uint64_t next_list = pivot;
                for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {
                }
                ordered_cursors[next_list]->next_geq(pivot_id);
                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() < ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

    std::vector<std::pair<float, uint64_t>> const& secondary_topk() const { return m_secondary.topk(); }

    std::vector<std::pair<float, uint64_t>> const& cyclic() const { return m_cyclic.topk(); }

  private:
    topk_queue& m_topk;
    topk_queue& m_secondary;
    cyclic_queue& m_cyclic;

};

}  // namespace pisa
