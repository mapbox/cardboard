// eslint.config.cjs
const js = require("@eslint/js");

module.exports = [
    js.configs.recommended,
    {
        languageOptions: {
            globals: {
                process: "readonly",
                module: "readonly",
                require: "readonly",
                __dirname: "readonly",
                setTimeout: "readonly",
            },
        },
        rules: {
            indent: ["error", 4],
            quotes: ["error", "single"],
            "no-console": "off",
            // "no-unused-vars": ["warn"],
        },
    },
];
