const express = require('express');
const app = express();
const fs = require('fs');
const JSONStream = require('jsonstream');

app.set('view engine', 'ejs');
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

app.get('/', (req, res) => {
    res.render('index', { result: null, error: null });
});

app.post('/search', (req, res) => {
    const searchType = req.body.searchType.trim();
    const searchNumber = req.body.searchNumber;

    if (searchType === 'policy') {
        fs.readFile('database.json', 'utf8', (err, data) => {
            if (err) {
                console.error(err);
                return res.render('index', { result: null, error: 'Error reading policy database.' });
            }
            const policies = JSON.parse(data).policy;
            const result = policies.find(p => p.Policy_Number === searchNumber);
            if (result) {
                res.render('details', { result: result, searchType: 'Policy', factDetails: null, pkValue: result.Insurance_PK });
            } else {
                res.render('index', { result: null, error: 'Policy not found.' });
            }
        });
    } else if (searchType === 'claim') {
        fs.readFile('database2.json', 'utf8', (err, data) => {
            if (err) {
                console.error(err);
                return res.render('index', { result: null, error: 'Error reading claim database.' });
            }
            const claims = JSON.parse(data).claim;
            const result = claims.find(c => c.Claim_Number === searchNumber);
            if (result) {
                res.render('details', { result: result, searchType: 'Claim', factDetails: null, pkValue: result.Claim_PK });
            } else {
                res.render('index', { result: null, error: 'Claim not found.' });
            }
        });
    } else if (searchType === 'state') {
        fs.readFile('database3.json', 'utf8', (err, data) => {
            if (err) {
                console.error(err);
                return res.render('index', { result: null, error: 'Error reading state database.' });
            }
            const states = JSON.parse(data).state;
            const result = states.find(s => s.Claim_State_PK === searchNumber);
            if (result) {
                res.render('details', { result: result, searchType: 'State', factDetails: null, pkValue: result.Claim_State_PK });
            } else {
                res.render('index', { result: null, error: 'State not found.' });
            }
        });
    } else {
        res.render('index', { result: null, error: 'Invalid search type.' });
    }
});

app.post('/getFactDetails', async (req, res) => {
    const searchType = req.body.searchType;
    const pkValue = req.body.pkValue;

    try {
        const policyData = JSON.parse(fs.readFileSync('database.json', 'utf8')).policy;
        const claimData = JSON.parse(fs.readFileSync('database2.json', 'utf8')).claim;
        const stateData = JSON.parse(fs.readFileSync('database3.json', 'utf8')).state;

        const joinedData = [];
        const factStream = fs.createReadStream('database4.json', { encoding: 'utf8' });
        const parser = JSONStream.parse('fact.*');

        factStream.pipe(parser);

        parser.on('data', (fact) => {
            const policy = policyData.find(p => p.Insurance_PK === fact.Insurance_PK);
            const claim = claimData.find(c => c.Claim_PK === fact.Claim_PK);
            const state = stateData.find(s => s.Claim_State_PK === fact.Claim_State_PK);

            joinedData.push({
                'Policy_Number': policy ? policy.Policy_Number : 'N/A',
                'Claim_Number': claim ? claim.Claim_Number : 'N/A',
                'Valuation_Code': state ? state.Valuation_Code : 'N/A',
                'Posting_Date': fact.Posting_Date,
                'Base_Amount': fact.Base_Amount,
                'Contract_Amount': fact.Contract_Amount,
                'Claim_State_PK': fact.Claim_State_PK
            });
        });

        parser.on('end', () => {
            let filteredData = joinedData;
            if (searchType === 'Policy') {
                const policy = policyData.find(p => p.Insurance_PK === pkValue);
                if (policy) {
                    filteredData = joinedData.filter(d => d.Policy_Number === policy.Policy_Number);
                }
            } else if (searchType === 'Claim') {
                const claim = claimData.find(c => c.Claim_PK === pkValue);
                if (claim) {
                    filteredData = joinedData.filter(d => d.Claim_Number === claim.Claim_Number);
                }
            } else if (searchType === 'State') {
                filteredData = joinedData.filter(d => d.Claim_State_PK === pkValue);
            }

            const page = parseInt(req.body.page) || 1;
            const pageSize = 5;
            const totalFacts = filteredData.length;
            const totalPages = Math.ceil(totalFacts / pageSize);
            const startIndex = (page - 1) * pageSize;
            const endIndex = page * pageSize;
            const factsForPage = filteredData.slice(startIndex, endIndex);

            let originalResult = null;
            if (searchType === 'Policy') {
                originalResult = policyData.find(p => p.Insurance_PK === pkValue);
            } else if (searchType === 'Claim') {
                originalResult = claimData.find(c => c.Claim_PK === pkValue);
            } else if (searchType === 'State') {
                originalResult = stateData.find(s => s.Claim_State_PK === pkValue);
            }

            res.render('details', {
                result: originalResult,
                searchType: searchType,
                factDetails: factsForPage,
                currentPage: page,
                totalPages: totalPages,
                totalFacts: totalFacts,
                pkValue: pkValue,
                joined: true
            });
        });

        parser.on('error', (err) => {
            console.error(err);
            res.render('index', { result: null, error: 'Error processing fact data.' });
        });

    } catch (err) {
        console.error(err);
        res.render('index', { result: null, error: 'Error processing data.' });
    }
});

app.listen(3000, () => {
    console.log('Server started on port 3000');
});