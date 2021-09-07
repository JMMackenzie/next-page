#pragma once

//NEXTPAGE: This implements the cyclic queue

#include "util/likely.hpp"
#include "util/util.hpp"
#include <algorithm>

namespace pisa {

using Threshold = float;

struct cyclic_queue {

    using entry_type = std::pair<float, uint64_t>;

    explicit cyclic_queue(uint64_t k) : m_k(k), m_index(0) { m_data.resize(m_k); }
    cyclic_queue(cyclic_queue const&) = default;
    cyclic_queue(cyclic_queue&&) noexcept = default;
    cyclic_queue& operator=(cyclic_queue const&) = default;
    cyclic_queue& operator=(cyclic_queue&&) noexcept = default;
    ~cyclic_queue() = default;

    [[nodiscard]] constexpr static auto
    min_heap_order(entry_type const& lhs, entry_type const& rhs) noexcept -> bool
    {
        return lhs.first > rhs.first;
    }

    Threshold threshold() const noexcept { return m_data[m_index].first; }

    
    // Finds the largest score which is <= threshold and returns the identifier
    // which caused this displacement
    uint64_t displaced_id(float threshold) const noexcept {

        size_t index = m_index;

        // Range: (m_index, end]
        for (size_t i = m_index + 1; i < m_data.size(); ++i) {
            if (m_data[i].first <= threshold)
                index = i;     
        }
        
        // Range: [0, m_index)
        for (size_t i = 0; i < m_index; ++i) {
            if (m_data[i].first <= threshold)
                index = i;     
        }
        return m_data[index].second;
    }

    void dump() {
        for(size_t i = 0; i < m_data.size(); ++i) {
            std::cerr << i << ":" << m_data[i].second << " " << m_data[i].first << "\n";
        }
    }

    void insert(float score, uint64_t docid) {
        // Update current element
        m_data[m_index].first = score;
        m_data[m_index].second = docid;
        // Increment and wrap
        m_index += 1;
        m_index = m_index % m_k; 
    }

    void finalize()
    {
        std::sort(m_data.begin(), m_data.end(), min_heap_order);
    }

    [[nodiscard]] std::vector<entry_type> const& topk() const noexcept { return m_data; }

    void clear() noexcept
    {
        m_data.clear();
        m_index = 0;
    }

    [[nodiscard]] size_t capacity() const noexcept { return m_k; }

    [[nodiscard]] size_t size() const noexcept { return m_data.size(); }


    private:
      uint64_t m_k;
      size_t m_index; 
      std::vector<entry_type> m_data;
};

} // namespace pisa
