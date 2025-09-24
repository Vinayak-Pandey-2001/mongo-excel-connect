// routes/search.js
const express = require("express");
const { Client } = require("@elastic/elasticsearch");

const router = express.Router();

// ES client
const es = new Client({
  node: "https://0620dabd16044863be94de92adb946bd.ap-south-1.aws.elastic-cloud.com:443",
  auth: {
    apiKey: "TnBHbGpaQUJ5ZFdaWkVrcXM1SVg6MmlaUG94d2lUdVMtU2NlQVlBdThHdw=="
  },
  requestTimeout: 120000 // ms (Python uses seconds → multiply by 1000)
});

// Your index
const ES_INDEX = "vz-platform-website-docs-index-v2";

// POST /search
router.post("/", async (req, res) => {
  try {
    const { numberInput1, multiselect1, multiselect2, textInput1 } = req.body;

    const query = {
      size: numberInput1,
      sort: [{ _score: { order: "desc" } }],
      _source: [
        "gstn",
        "isActive",
        "companyName",
        "serviceDescription",
        "turnOverSlab",
        "products",
        "keyEquipments",
        "aboutUs",
        "discipline",
        "info_content",
        "experiences.description",
        "experiences.clientIndustry",
        "experiences.clientCompanyName",
        "im_about_us",
        "im_product_svc.description",
        "emails",
        "contactNumbers",
        "email",
        "phoneNumber",
        "website",
        "registeredStates",
        "locationServedStates",
        "industriesServed",
        "turnoverSlabFY",
        "registeredCities",
        "shortlistedInJobs_text",
        "breadth.registeredStates",
        "breadth.turnOverSlab",
        "im_url",
        "im_company_title",
        "breadth.companyName",
        "breadth.emails",
        "breadth.phoneNumbers"
      ],
      track_scores: true,
      query: {
        bool: {
          filter: [
            {
              bool: {
                should: [
                  { terms: { registeredStates: multiselect1 } },
                  { terms: { "breadth.registeredStates": multiselect1 } }
                ],
                minimum_should_match: 1
              }
            },
            {
              bool: {
                should: [
                  { terms: { turnOverSlab: multiselect2 } },
                  { terms: { "breadth.turnOverSlab": multiselect2 } }
                ],
                minimum_should_match: 1
              }
            }
          ],
          should: [
            {
              has_child: {
                type: "vendor_info",
                score_mode: "max",
                min_children: 1,
                query: {
                  dis_max: {
                    queries: [
                      {
                        simple_query_string: {
                          query: textInput1,
                          fields: [
                            "info_content.english_text^6",
                            "info_content.phrase_basic^4",
                            "info_content.phrase_match_shingles^8",
                            "info_content.phrase_match_stopwords^3"
                          ],
                          default_operator: "and",
                          flags: "ALL",
                          analyze_wildcard: true,
                          lenient: true
                        }
                      }
                    ]
                  }
                },
                inner_hits: {
                  _source: false,
                  size: 5,
                  highlight: {
                    fields: {
                      "info_content.english_text": {},
                      "info_content.phrase_match_shingles": {}
                    }
                  },
                  sort: [{ _score: { order: "desc" } }]
                }
              }
            },
            {
              dis_max: {
                tie_breaker: 0.2,
                queries: [
                  {
                    simple_query_string: {
                      query: textInput1,
                      fields: [
                        "combined_text.english_text^12",
                        "aboutUs.english_text^10",
                        "serviceDescription.english_text^8",
                        "experiences.description.english_text^6",
                        "im_about_us.english_text^6",
                        "im_product_svc.description^6",
                        "discipline.phrase_basic^5",
                        "products.phrase_basic^5",
                        "keyEquipments.phrase_basic^5",
                        "experiences.clientIndustry.phrase_basic^4",
                        "experiences.clientCompanyName.phrase_basic^4",
                        "shortlistedInJobs_text^12",
                        "im_product_svc_keys^6"
                      ],
                      default_operator: "and",
                      flags: "ALL",
                      analyze_wildcard: true,
                      lenient: true
                    }
                  }
                ]
              }
            }
          ],
          minimum_should_match: 1
        }
      },
      highlight: {
        pre_tags: ["<em>"],
        post_tags: ["</em>"],
        fields: {
          shortlistedInJobs_text: {},
          "products.phrase_basic": {},
          "keyEquipments.phrase_basic": {},
          "aboutUs.english_text": {},
          "im_about_us.english_text": {},
          im_product_svc_keys: {},
          "im_product_svc.description": {},
          "combined_text.english_text": {},
          info_content: {},
          "experiences.description.english_text": {},
          "experiences.clientIndustry.phrase_basic": {},
          "experiences.clientCompanyName.phrase_basic": {},
          "discipline.phrase_basic": {},
          "serviceDescription.english_text": {},
          im_product_images: {}
        },
        max_analyzed_offset: 1000000
      }
    };

    const response = await es.search({
      index: ES_INDEX,
      body: query
    });

    res.json(response.hits);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Elasticsearch query failed", details: err.message });
  }
});

module.exports = router;