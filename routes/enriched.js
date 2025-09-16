const express = require("express");
const router = express.Router();

router.get("/api/longlist/enriched", async (req, res) => {
  try {
    const skip = parseInt(req.query.skip) || 0;
    const limit = parseInt(req.query.limit) || 10000;

    const client = req.app.locals.client;
    const vendorDB = client.db("vendor-leads");
    const misDB = client.db("client-profile");
    const communicationDB = client.db("communication")

    // 1️⃣ Query vendorleadslonglistforjob with jobId filters
    const vendorQuery = {
      jobId: {
        $gte: "JR-2500",
        $lt: "JR-6000",
        $not: /(OJR|JR-JR)/,
      },
    };

    const vendorFieldsProjection = {
      domain_name: 1,
      gstn: 1,
      jobId: 1,
      generation: 1,
      quote_status: 1,
      remarks: 1,
      shortlist_status: 1,
      turnover_slab: 1,
      quote_v1_received_timestamp: 1,
      won_or_lost_status: 1,
    //   Quote_Received: 1,
    //  quoteValueInInr: 1,
      quote_submitted_to_client: 1,
      quote_submitted_to_client_timestamp: 1,
      f3_published_for_Client: 1,
      f2_published_for_Client: 1,
      lowestQuoteValue: 1,
      push_timestamp: 1,
      isL3Vendor: 1,
      availableDocsCount: 1,
      shortlist_timestamp: 1,
      addedToL3At: 1
    };

    const vendorDocs = await vendorDB
      .collection("vendorleadslonglistforjob")
      .find(vendorQuery)
      .skip(skip)
    //   .limit(limit)
      .project(vendorFieldsProjection)
      .toArray();

    if (!vendorDocs.length) return res.json([]);

    // 2️⃣ Collect unique jobIds and fetch jobIntent info
    const jobIds = [...new Set(vendorDocs.map(doc => doc.jobId))];
    console.log("Longlist JOB IDs :",jobIds);
    const jobInfoDocs = await misDB
      .collection("job-mis-tracking")
      .find({ jobId: { $in: jobIds } })
      .project({ jobId: 1, jobIntent: 1 })
      .toArray();

    const jobInfoMap = {};
    jobInfoDocs.forEach(info => {
      jobInfoMap[info.jobId] = info;
    });

    // 3️⃣ Filter vendorDocs by jobIntent == "Live"
    const liveVendorDocs = vendorDocs.filter(doc => {
      const jobInfo = jobInfoMap[doc.jobId];
      return jobInfo && jobInfo.jobIntent === "live";
    });

    console.log("Live Jobs :",liveVendorDocs);

    if (!liveVendorDocs.length) return res.json([]);

    // 4️⃣ Derive PANs for companyName mapping
    const panSet = new Set();
    const enrichedDocs = liveVendorDocs.map(doc => {
      let pan = null;

      if (doc.gstn) {
        pan = doc.gstn.substring(2, 12).toUpperCase();
      } else if (doc.domain_name && doc.domain_name!=" " && doc.domain_name!=null) {
        const gstnMatch = doc.domain_name.match(/[A-Z0-9]{15}/i);
        if (gstnMatch){
            const panMatch = gstnMatch[0].substring(2,12);
            if (panMatch) pan = panMatch.toUpperCase();
        }
      }

      if (pan) panSet.add(pan);
      return { ...doc, derivedPAN: pan };
    });

    const uniquePANs = Array.from(panSet);

    const quotationDocs = await communicationDB
      .collection("quotations")
      .find({ jobRequestId: { $gte: "JR-2500" } })
      .project({ jobRequestId: 1, requested_to: 1, technicalFiles: 1, commercialFiles: 1, firstSubmission: 1 })
      .toArray();

    // 5️⃣ Fetch company names from centralized_vendor_pool
    const cvpDocs = await vendorDB
      .collection("centralized_vendor_pool")
      .find({ pan: { $in: uniquePANs } })
      .project({ pan: 1, companyName: 1 })
      .toArray();

    const panToCompanyMap = {};
    cvpDocs.forEach(doc => {
      panToCompanyMap[doc.pan] = doc.companyName;
    });
    
    const gstnToQuotationMap = {};
    quotationDocs.forEach(doc => {
      gstnToQuotationMap[doc.requested_to] = {
        jobRequestId: doc.jobRequestId,
        technicalFiles: doc.technicalFiles,
        commercialFiles: doc.commercialFiles,
        firstSubmission: doc.firstSubmission
      };
    });

    // 6️⃣ Attach companyName to enriched docs
    const finalDocs = enrichedDocs.map(doc => {
      const companyName = doc.derivedPAN ? panToCompanyMap[doc.derivedPAN] || null : null;
      const quotationInfo = gstnToQuotationMap[doc.gstn] || null;

      let f3_published_details = "";
      let f2_published_details = "";
      let vendor_document_count = "";
      let f3_addition_and_quote_details = "";

      if (
        doc.f2_published_for_Client &&
        doc.f2_published_for_Client.published === true &&
        doc.f2_published_for_Client.timestamp
      ) {
        const dateObj = new Date(doc.f2_published_for_Client.timestamp);
        const options = {
          day: "2-digit",
          month: "short",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: true,
          timeZone: "Asia/Kolkata"
        };
        const formattedDate = dateObj.toLocaleString("en-GB", options).replace(",", "").replace(" at", ",");

        f2_published_details = `${companyName || "Unknown"}  -  ${formattedDate}`;
      }
      if (
        doc.f3_published_for_Client &&
        doc.f3_published_for_Client.published === true &&
        doc.f3_published_for_Client.timestamp
      ) {
        const dateObj = new Date(doc.f3_published_for_Client.timestamp);
        const options = {
          day: "2-digit",
          month: "short",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: true,
          timeZone: "Asia/Kolkata"
        };
        const formattedDate = dateObj.toLocaleString("en-GB", options).replace(",", "").replace(" at", ",");

        const formattedQuoteValue = doc.lowestQuoteValue ? `INR ${doc.lowestQuoteValue}`: "Not yet Quoted";

        f3_published_details = `${companyName || "Unknown"}  -  ${formattedQuoteValue}  -  ${formattedDate}`;
      }
      if (
        doc.availableDocsCount &&
        doc.availableDocsCount > 0 &&
        doc.shortlist_status === true
      ) {
        const dateObj = new Date(doc.shortlist_timestamp);
        const options = {
          day: "2-digit",
          month: "short",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: true,
          timeZone: "Asia/Kolkata"
        };
        const formattedDate = dateObj.toLocaleString("en-GB", options).replace(",", "").replace(" at", ",");

        const docCount = doc.availableDocsCount;

        vendor_document_count = `${companyName || "Unknown"}  -  ${formattedDate}  -  ${docCount}`;
      }
      if (
        doc.isL3Vendor &&
        doc.isL3Vendor === true
      ) {

        const dateObj = new Date(doc.addedToL3At);
        const options = {
          day: "2-digit",
          month: "short",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: true,
          timeZone: "Asia/Kolkata"
        };

        const f3AddedAt = dateObj.toLocaleString("en-GB", options).replace(",", "").replace(" at", ",");
        let firstQuoteSubmissionformattedDate = null;

        if (quotationInfo.technicalFiles || quotationInfo.commercialFiles){
          const dateObj = new Date(quotationInfo.firstSubmission.date);
          const options = {
            day: "2-digit",
            month: "short",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: true,
            timeZone: "Asia/Kolkata"
          };
          firstQuoteSubmissionformattedDate = dateObj.toLocaleString("en-GB", options).replace(",", "").replace(" at", ",");
        }

        let formattedQuoteValue;
        if (!doc.lowestQuoteValue) {
          if (!firstQuoteSubmissionformattedDate) {
            formattedQuoteValue = "Not yet Quoted";
          } else {
            formattedQuoteValue = "Data not Logged by user";
          }
        } else {
          formattedQuoteValue = `INR ${doc.lowestQuoteValue}`;
        }

        f3_addition_and_quote_details = `${companyName || "Unknown"}  -  ${formattedQuoteValue}  -  ${f3AddedAt} - ${firstQuoteSubmissionformattedDate || "Not yet Quoted"}`;
      }
      
      return {
        ...doc,
        companyName,
        f2_published_details,
        f3_published_details,
        vendor_document_count,
        f3_addition_and_quote_details
      };
    });

    res.json(finalDocs);

  } catch (err) {
    console.error("❌ Enriched API error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

module.exports = router;