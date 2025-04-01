
const PORT = 7001

const path = require('/usr/local/lib/node_modules/path');
const dotenv = require('/usr/local/lib/node_modules/dotenv');
dotenv.config();

const express = require('/usr/local/lib/node_modules/express');
const admin = require('./_router/admin');
const router = require('./_router/router');
const app = express();

function handle404(req, res) {
    res.status(404).sendFile('/usr/share/nginx/html/404.html');
}

app.use((req, res, next)=> {
    console.log('[trace]\t', req.headers['x-forwarded-for'], req.url);
    next();
});

app.use(admin);
app.use(router);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/recce', express.static(path.join(__dirname, '_public')));

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'view'));

app.get('*', handle404);

app.listen(PORT, ()=> {
    console.log(' [info]\t', 'listening on', '217.154.54.14:'+PORT);
});
