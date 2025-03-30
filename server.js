
const dotenv = require('/usr/local/lib/node_modules/dotenv');
dotenv.config();

const express = require('/usr/local/lib/node_modules/express');
const admin = require('./_router/admin');
const router = require('./_router/router');
const app = express();

app.use(admin);
app.use(router);
app.use('/recce', express.static('_public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next)=> {
    console.log('[TRACE]\t', req.headers['x-forwarded-for'], req.url);
    next();
});

app.get('*', (req, res)=> {
    res.status(404).sendFile('/usr/share/nginx/html/404.html');
});

app.listen(process.env.PORT, ()=> {
    console.log(' [info]', 'listening on', '217.154.54.14:'+process.env.PORT);
});
