var gD=Object.defineProperty,vD=Object.defineProperties;var yD=Object.getOwnPropertyDescriptors;var tv=Object.getOwnPropertySymbols;var _D=Object.prototype.hasOwnProperty,bD=Object.prototype.propertyIsEnumerable;var nv=(t,n,e)=>n in t?gD(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})_D.call(n,e)&&nv(t,e,n[e]);if(tv)for(var e of tv(n))bD.call(n,e)&&nv(t,e,n[e]);return t},j=(t,n)=>vD(t,yD(n));var _e=(t,n,e)=>new Promise((i,r)=>{var o=c=>{try{a(e.next(c))}catch(l){r(l)}},s=c=>{try{a(e.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((e=e.apply(t,n)).next())});var It=null,Tc=!1,dr=1,CD=null,Be=Symbol("SIGNAL");function B(t){let n=It;return It=t,n}function Ac(){return It}var Ti={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ai(t){if(Tc)throw new Error("");if(It===null)return;It.consumerOnSignalRead(t);let n=It.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=It.recomputing;if(i&&(e=n!==void 0?n.nextProducer:It.producers,e!==void 0&&e.producer===t)){It.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=dr;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===It&&(!i||r.knownValidAtEpoch===dr))return;let o=yo(It),s={producer:t,consumer:It,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:dr,lastReadVersion:t.version,nextConsumer:void 0};It.producersTail=s,n!==void 0?n.nextProducer=s:It.producers=s,o&&sv(t,s)}function iv(){dr++}function hr(t){if(!(yo(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===dr)){if(!t.producerMustRecompute(t)&&!vo(t)){go(t);return}t.producerRecomputeValue(t),go(t)}}function cf(t){if(t.consumers===void 0)return;let n=Tc;Tc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||ED(i)}}finally{Tc=n}}function lf(){return It?.consumerAllowSignalWrites!==!1}function ED(t){t.dirty=!0,cf(t),t.consumerMarkedDirty?.(t)}function go(t){t.dirty=!1,t.lastCleanEpoch=dr}function oi(t){return t&&rv(t),B(t)}function rv(t){if(t.producersTail?.knownValidAtEpoch===dr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Ri(t,n){B(n),t&&ov(t)}function ov(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(yo(t))do e=df(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function vo(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(hr(e),i!==e.version))return!0}return!1}function Ni(t){if(yo(t)){let n=t.producers;for(;n!==void 0;)n=df(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function sv(t,n){let e=t.consumersTail,i=yo(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)sv(r.producer,r)}function df(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!yo(n)){let o=n.producers;for(;o!==void 0;)o=df(o)}return e}function yo(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Ts(t){CD?.(t)}function As(t,n){return Object.is(t,n)}function Rs(t,n){let e=Object.create(wD);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(hr(e),Ai(e),e.value===Pn)throw e.error;return e.value};return i[Be]=e,Ts(e),i}var ur=Symbol("UNSET"),fr=Symbol("COMPUTING"),Pn=Symbol("ERRORED"),wD=j(C({},Ti),{value:ur,dirty:!0,error:null,equal:As,kind:"computed",producerMustRecompute(t){return t.value===ur||t.value===fr},producerRecomputeValue(t){if(t.value===fr)throw new Error("");let n=t.value;t.value=fr;let e=oi(t),i,r=!1;try{i=t.computation(),B(null),r=n!==ur&&n!==Pn&&i!==Pn&&t.equal(n,i)}catch(o){i=Pn,t.error=o}finally{Ri(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function DD(){throw new Error}var av=DD;function cv(t){av(t)}function uf(t){av=t}var xD=null;function ff(t,n){let e=Object.create(Ns);e.value=t,n!==void 0&&(e.equal=n);let i=()=>lv(e);return i[Be]=e,Ts(e),[i,s=>pr(e,s),s=>Rc(e,s)]}function lv(t){return Ai(t),t.value}function pr(t,n){lf()||cv(t),t.equal(t.value,n)||(t.value=n,ID(t))}function Rc(t,n){lf()||cv(t),pr(t,n(t.value))}var Ns=j(C({},Ti),{equal:As,value:void 0,kind:"signal"});function ID(t){t.version++,iv(),cf(t),xD?.(t)}var hf=j(C({},Ti),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function pf(t){if(t.dirty=!1,t.version>0&&!vo(t))return;t.version++;let n=oi(t);try{t.cleanup(),t.fn()}finally{Ri(t,n)}}var mf;function Nc(){return mf}function Ln(t){let n=mf;return mf=t,n}var dv=Symbol("NotFound");function _o(t){return t===dv||t?.name==="\u0275NotFound"}function gf(t,n,e){let i=Object.create(SD);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(hr(i),Ai(i),i.value===Pn)throw i.error;return i.value};return o[Be]=i,Ts(i),o}function uv(t,n){hr(t),pr(t,n),go(t)}function fv(t,n){if(hr(t),t.value===Pn)throw t.error;Rc(t,n),go(t)}var SD=j(C({},Ti),{value:ur,dirty:!0,error:null,equal:As,kind:"linkedSignal",producerMustRecompute(t){return t.value===ur||t.value===fr},producerRecomputeValue(t){if(t.value===fr)throw new Error("");let n=t.value;t.value=fr;let e=oi(t),i,r=!1;try{let o=t.source(),s=n!==ur&&n!==Pn,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,B(null),r=s&&i!==Pn&&t.equal(n,i)}catch(o){i=Pn,t.error=o}finally{Ri(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function hv(t){let n=B(null);try{return t()}finally{B(n)}}function Z(t){return typeof t=="function"}function bo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var kc=bo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function mr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var de=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(Z(i))try{i()}catch(o){n=o instanceof kc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{pv(o)}catch(s){n=n??[],s instanceof kc?n=[...n,...s.errors]:n.push(s)}}if(n)throw new kc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)pv(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&mr(e,n)}remove(n){let{_finalizers:e}=this;e&&mr(e,n),n instanceof t&&n._removeParent(this)}};de.EMPTY=(()=>{let t=new de;return t.closed=!0,t})();var vf=de.EMPTY;function Oc(t){return t instanceof de||t&&"closed"in t&&Z(t.remove)&&Z(t.add)&&Z(t.unsubscribe)}function pv(t){Z(t)?t():t.unsubscribe()}var In={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Co={setTimeout(t,n,...e){let{delegate:i}=Co;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Co;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Fc(t){Co.setTimeout(()=>{let{onUnhandledError:n}=In;if(n)n(t);else throw t})}function ks(){}var mv=yf("C",void 0,void 0);function gv(t){return yf("E",void 0,t)}function vv(t){return yf("N",t,void 0)}function yf(t,n,e){return{kind:t,value:n,error:e}}var gr=null;function Eo(t){if(In.useDeprecatedSynchronousErrorHandling){let n=!gr;if(n&&(gr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=gr;if(gr=null,e)throw i}}else t()}function yv(t){In.useDeprecatedSynchronousErrorHandling&&gr&&(gr.errorThrown=!0,gr.error=t)}var vr=class extends de{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Oc(n)&&n.add(this)):this.destination=AD}static create(n,e,i){return new si(n,e,i)}next(n){this.isStopped?bf(vv(n),this):this._next(n)}error(n){this.isStopped?bf(gv(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?bf(mv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},MD=Function.prototype.bind;function _f(t,n){return MD.call(t,n)}var Cf=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Pc(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Pc(i)}else Pc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Pc(e)}}},si=class extends vr{constructor(n,e,i){super();let r;if(Z(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&In.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&_f(n.next,o),error:n.error&&_f(n.error,o),complete:n.complete&&_f(n.complete,o)}):r=n}this.destination=new Cf(r)}};function Pc(t){In.useDeprecatedSynchronousErrorHandling?yv(t):Fc(t)}function TD(t){throw t}function bf(t,n){let{onStoppedNotification:e}=In;e&&Co.setTimeout(()=>e(t,n))}var AD={closed:!0,next:ks,error:TD,complete:ks};var wo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function qt(t){return t}function Ef(...t){return wf(t)}function wf(t){return t.length===0?qt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ee=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=ND(e)?e:new si(e,i,r);return Eo(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=_v(i),new i((r,o)=>{let s=new si({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[wo](){return this}pipe(...e){return wf(e)(this)}toPromise(e){return e=_v(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function _v(t){var n;return(n=t??In.Promise)!==null&&n!==void 0?n:Promise}function RD(t){return t&&Z(t.next)&&Z(t.error)&&Z(t.complete)}function ND(t){return t&&t instanceof vr||RD(t)&&Oc(t)}function kD(t){return Z(t?.lift)}function K(t){return n=>{if(kD(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function te(t,n,e,i,r){return new Df(t,n,e,i,r)}var Df=class extends vr{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var bv=bo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var I=(()=>{class t extends ee{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Lc(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new bv}next(e){Eo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Eo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Eo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?vf:(this.currentObservers=null,o.push(e),new de(()=>{this.currentObservers=null,mr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ee;return e.source=this,e}}return t.create=(n,e)=>new Lc(n,e),t})(),Lc=class extends I{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:vf}};var et=class extends I{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Os={now(){return(Os.delegate||Date).now()},delegate:void 0};var Vc=class extends I{constructor(n=1/0,e=1/0,i=Os){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var jc=class extends de{constructor(n,e){super()}schedule(n,e=0){return this}};var Fs={setInterval(t,n,...e){let{delegate:i}=Fs;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Fs;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Bc=class extends jc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Fs.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Fs.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,mr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Do=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Do.now=Os.now;var Uc=class extends Do{constructor(n,e=Do.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Ps=new Uc(Bc),Cv=Ps;var Ue=new ee(t=>t.complete());function Hc(t){return t&&Z(t.schedule)}function xf(t){return t[t.length-1]}function zc(t){return Z(xf(t))?t.pop():void 0}function Vn(t){return Hc(xf(t))?t.pop():void 0}function Ev(t,n){return typeof xf(t)=="number"?t.pop():n}function Dv(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(f){s(f)}}function c(d){try{l(i.throw(d))}catch(f){s(f)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function wv(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function yr(t){return this instanceof yr?(this.v=t,this):new yr(t)}function xv(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(g){return function(b){return Promise.resolve(b).then(g,f)}}function a(g,b){i[g]&&(r[g]=function(w){return new Promise(function(R,J){o.push([g,w,R,J])>1||c(g,w)})},b&&(r[g]=b(r[g])))}function c(g,b){try{l(i[g](b))}catch(w){h(o[0][3],w)}}function l(g){g.value instanceof yr?Promise.resolve(g.value.v).then(d,f):h(o[0][2],g)}function d(g){c("next",g)}function f(g){c("throw",g)}function h(g,b){g(b),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Iv(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof wv=="function"?wv(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var $c=t=>t&&typeof t.length=="number"&&typeof t!="function";function Gc(t){return Z(t?.then)}function Wc(t){return Z(t[wo])}function qc(t){return Symbol.asyncIterator&&Z(t?.[Symbol.asyncIterator])}function Yc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function OD(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Zc=OD();function Kc(t){return Z(t?.[Zc])}function Qc(t){return xv(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield yr(e.read());if(r)return yield yr(void 0);yield yield yr(i)}}finally{e.releaseLock()}})}function Xc(t){return Z(t?.getReader)}function we(t){if(t instanceof ee)return t;if(t!=null){if(Wc(t))return FD(t);if($c(t))return PD(t);if(Gc(t))return LD(t);if(qc(t))return Sv(t);if(Kc(t))return VD(t);if(Xc(t))return jD(t)}throw Yc(t)}function FD(t){return new ee(n=>{let e=t[wo]();if(Z(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function PD(t){return new ee(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function LD(t){return new ee(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Fc)})}function VD(t){return new ee(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Sv(t){return new ee(n=>{BD(t,n).catch(e=>n.error(e))})}function jD(t){return Sv(Qc(t))}function BD(t,n){var e,i,r,o;return Dv(this,void 0,void 0,function*(){try{for(e=Iv(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Ot(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Jc(t,n=0){return K((e,i)=>{e.subscribe(te(i,r=>Ot(i,t,()=>i.next(r),n),()=>Ot(i,t,()=>i.complete(),n),r=>Ot(i,t,()=>i.error(r),n)))})}function el(t,n=0){return K((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Mv(t,n){return we(t).pipe(el(n),Jc(n))}function Tv(t,n){return we(t).pipe(el(n),Jc(n))}function Av(t,n){return new ee(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function Rv(t,n){return new ee(e=>{let i;return Ot(e,n,()=>{i=t[Zc](),Ot(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>Z(i?.return)&&i.return()})}function tl(t,n){if(!t)throw new Error("Iterable cannot be null");return new ee(e=>{Ot(e,n,()=>{let i=t[Symbol.asyncIterator]();Ot(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Nv(t,n){return tl(Qc(t),n)}function kv(t,n){if(t!=null){if(Wc(t))return Mv(t,n);if($c(t))return Av(t,n);if(Gc(t))return Tv(t,n);if(qc(t))return tl(t,n);if(Kc(t))return Rv(t,n);if(Xc(t))return Nv(t,n)}throw Yc(t)}function Ae(t,n){return n?kv(t,n):we(t)}function L(...t){let n=Vn(t);return Ae(t,n)}function Ls(t,n){let e=Z(t)?t:()=>t,i=r=>r.error(e());return new ee(n?r=>n.schedule(i,0,r):i)}function Vs(t){return!!t&&(t instanceof ee||Z(t.lift)&&Z(t.subscribe))}var _r=bo(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Ov(t){return t instanceof Date&&!isNaN(t)}function Q(t,n){return K((e,i)=>{let r=0;e.subscribe(te(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:UD}=Array;function HD(t,n){return UD(n)?t(...n):t(n)}function nl(t){return Q(n=>HD(t,n))}var{isArray:zD}=Array,{getPrototypeOf:$D,prototype:GD,keys:WD}=Object;function il(t){if(t.length===1){let n=t[0];if(zD(n))return{args:n,keys:null};if(qD(n)){let e=WD(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function qD(t){return t&&typeof t=="object"&&$D(t)===GD}function rl(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function If(...t){let n=Vn(t),e=zc(t),{args:i,keys:r}=il(t);if(i.length===0)return Ae([],n);let o=new ee(YD(i,n,r?s=>rl(r,s):qt));return e?o.pipe(nl(e)):o}function YD(t,n,e=qt){return i=>{Fv(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)Fv(n,()=>{let l=Ae(t[c],n),d=!1;l.subscribe(te(i,f=>{o[c]=f,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Fv(t,n,e){t?Ot(e,t,n):n()}function Pv(t,n,e,i,r,o,s,a){let c=[],l=0,d=0,f=!1,h=()=>{f&&!c.length&&!l&&n.complete()},g=w=>l<i?b(w):c.push(w),b=w=>{o&&n.next(w),l++;let R=!1;we(e(w,d++)).subscribe(te(n,J=>{r?.(J),o?g(J):n.next(J)},()=>{R=!0},void 0,()=>{if(R)try{for(l--;c.length&&l<i;){let J=c.shift();s?Ot(n,s,()=>b(J)):b(J)}h()}catch(J){n.error(J)}}))};return t.subscribe(te(n,g,()=>{f=!0,h()})),()=>{a?.()}}function St(t,n,e=1/0){return Z(n)?St((i,r)=>Q((o,s)=>n(i,o,r,s))(we(t(i,r))),e):(typeof n=="number"&&(e=n),K((i,r)=>Pv(i,r,t,e)))}function ol(t=1/0){return St(qt,t)}function Lv(){return ol(1)}function xo(...t){return Lv()(Ae(t,Vn(t)))}function br(t){return new ee(n=>{we(t()).subscribe(n)})}function js(...t){let n=zc(t),{args:e,keys:i}=il(t),r=new ee(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let f=!1;we(e[d]).subscribe(te(o,h=>{f||(f=!0,l--),a[d]=h},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?rl(i,a):a),o.complete())}))}});return n?r.pipe(nl(n)):r}function Vv(t=0,n,e=Cv){let i=-1;return n!=null&&(Hc(n)?e=n:i=n),new ee(r=>{let o=Ov(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function jn(...t){let n=Vn(t),e=Ev(t,1/0),i=t;return i.length?i.length===1?we(i[0]):ol(e)(Ae(i,n)):Ue}function Me(t,n){return K((e,i)=>{let r=0;e.subscribe(te(i,o=>t.call(n,o,r++)&&i.next(o)))})}function jv(t){return K((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(te(e,l=>{i=!0,r=l,o||we(t(l)).subscribe(o=te(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function sl(t,n=Ps){return jv(()=>Vv(t,n))}function Cr(t){return K((n,e)=>{let i=null,r=!1,o;i=n.subscribe(te(e,void 0,void 0,s=>{o=we(t(s,Cr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Io(t,n){return Z(n)?St(t,n,1):St(t,1)}function Sf(t,n=Ps){return K((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}e.subscribe(te(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Bv(t){return K((n,e)=>{let i=!1;n.subscribe(te(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Yt(t){return t<=0?()=>Ue:K((n,e)=>{let i=0;n.subscribe(te(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function al(t,n=qt){return t=t??ZD,K((e,i)=>{let r,o=!0;e.subscribe(te(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function ZD(t,n){return t===n}function Uv(t=KD){return K((n,e)=>{let i=!1;n.subscribe(te(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function KD(){return new _r}function Er(t){return K((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function ai(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Me((r,o)=>t(r,o,i)):qt,Yt(1),e?Bv(n):Uv(()=>new _r))}function cl(t){return t<=0?()=>Ue:K((n,e)=>{let i=[];n.subscribe(te(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function ll(){return K((t,n)=>{let e,i=!1;t.subscribe(te(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Bs(t={}){let{connector:n=()=>new I,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,d=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},g=()=>{h(),s=c=void 0,d=f=!1},b=()=>{let w=s;g(),w?.unsubscribe()};return K((w,R)=>{l++,!f&&!d&&h();let J=c=c??n();R.add(()=>{l--,l===0&&!f&&!d&&(a=Mf(b,r))}),J.subscribe(R),!s&&l>0&&(s=new si({next:Je=>J.next(Je),error:Je=>{f=!0,h(),a=Mf(g,e,Je),J.error(Je)},complete:()=>{d=!0,h(),a=Mf(g,i),J.complete()}}),we(w).subscribe(s))})(o)}}function Mf(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new si({next:()=>{i.unsubscribe(),t()}});return we(n(...e)).subscribe(i)}function dl(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Bs({connector:()=>new Vc(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Tf(t){return Me((n,e)=>t<=e)}function Zt(...t){let n=Vn(t);return K((e,i)=>{(n?xo(t,e,n):xo(t,e)).subscribe(i)})}function tt(t,n){return K((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(te(i,c=>{r?.unsubscribe();let l=0,d=o++;we(t(c,d)).subscribe(r=te(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ne(t){return K((n,e)=>{we(t).subscribe(te(e,()=>e.complete(),ks)),!e.closed&&n.subscribe(e)})}function Af(t,n=!1){return K((e,i)=>{let r=0;e.subscribe(te(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function He(t,n,e){let i=Z(t)||n||e?{next:t,error:n,complete:e}:t;return i?K((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(te(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):qt}var vl="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",E=class extends Error{code;constructor(n,e){super(di(n,e)),this.code=n}};function QD(t){return`NG0${Math.abs(t)}`}function di(t,n){return`${QD(t)}${n?": "+n:""}`}function ve(t){for(let n in t)if(t[n]===ve)return n;throw Error("")}function Wv(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Ws(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Ws).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function yl(t,n){return t?n?`${t} ${n}`:t:n||""}var XD=ve({__forward_ref__:ve});function cn(t){return t.__forward_ref__=cn,t}function dt(t){return zf(t)?t():t}function zf(t){return typeof t=="function"&&t.hasOwnProperty(XD)&&t.__forward_ref__===cn}function F(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function z(t){return{providers:t.providers||[],imports:t.imports||[]}}function qs(t){return JD(t,_l)}function $f(t){return qs(t)!==null}function JD(t,n){return t.hasOwnProperty(n)&&t[n]||null}function ex(t){let n=t?.[_l]??null;return n||null}function Nf(t){return t&&t.hasOwnProperty(fl)?t[fl]:null}var _l=ve({\u0275prov:ve}),fl=ve({\u0275inj:ve}),_=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=F({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Gf(t){return t&&!!t.\u0275providers}var Wf=ve({\u0275cmp:ve}),qf=ve({\u0275dir:ve}),Yf=ve({\u0275pipe:ve}),Zf=ve({\u0275mod:ve}),Hs=ve({\u0275fac:ve}),Mr=ve({__NG_ELEMENT_ID__:ve}),Hv=ve({__NG_ENV_ID__:ve});function qv(t){return bl(t,"@NgModule"),t[Zf]||null}function Oi(t){return bl(t,"@Component"),t[Wf]||null}function Kf(t){return bl(t,"@Directive"),t[qf]||null}function Yv(t){return bl(t,"@Pipe"),t[Yf]||null}function bl(t,n){if(t==null)throw new E(-919,!1)}function Cl(t){return typeof t=="string"?t:t==null?"":String(t)}var Zv=ve({ngErrorCode:ve}),tx=ve({ngErrorMessage:ve}),nx=ve({ngTokenPath:ve});function Qf(t,n){return Kv("",-200,n)}function El(t,n){throw new E(-201,!1)}function Kv(t,n,e){let i=new E(n,t);return i[Zv]=n,i[tx]=t,e&&(i[nx]=e),i}function ix(t){return t[Zv]}var kf;function Qv(){return kf}function Kt(t){let n=kf;return kf=t,n}function Xf(t,n,e){let i=qs(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;El(t,"")}var Fi=globalThis;var rx={},wr=rx,ox="__NG_DI_FLAG__",Of=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Dr(e)||0;try{return this.injector.get(n,i&8?null:wr,i)}catch(r){if(_o(r))return r;throw r}}};function sx(t,n=0){let e=Nc();if(e===void 0)throw new E(-203,!1);if(e===null)return Xf(t,void 0,n);{let i=ax(n),r=e.retrieve(t,i);if(_o(r)){if(i.optional)return null;throw r}return r}}function M(t,n=0){return(Qv()||sx)(dt(t),n)}function u(t,n){return M(t,Dr(n))}function Dr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function ax(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Ff(t){let n=[];for(let e=0;e<t.length;e++){let i=dt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new E(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=cx(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(M(r,o))}else n.push(M(i))}return n}function cx(t){return t[ox]}function xr(t,n){let e=t.hasOwnProperty(Hs);return e?t[Hs]:null}function Xv(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Jv(t){return t.flat(Number.POSITIVE_INFINITY)}function wl(t,n){t.forEach(e=>Array.isArray(e)?wl(e,n):n(e))}function Jf(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Ys(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function ey(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function ty(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Dl(t,n,e){let i=To(t,n);return i>=0?t[i|1]=e:(i=~i,ty(t,i,n,e)),i}function xl(t,n){let e=To(t,n);if(e>=0)return t[e|1]}function To(t,n){return lx(t,n,1)}function lx(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Pi={},yt=[],Tr=new _(""),Zs=new _("",-1),eh=new _(""),Mo=class{get(n,e=wr){if(e===wr){let r=Kv("",-201);throw r.name="\u0275NotFound",r}return e}};function ui(t){return{\u0275providers:t}}function ny(t){return ui([{provide:Tr,multi:!0,useValue:t}])}function iy(...t){return{\u0275providers:th(!0,t),\u0275fromNgModule:!0}}function th(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return wl(n,s=>{let a=s;hl(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&ry(r,o),e}function ry(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];nh(r,o=>{n(o,i)})}}function hl(t,n,e,i){if(t=dt(t),!t)return!1;let r=null,o=Nf(t),s=!o&&Oi(t);if(!o&&!s){let c=t.ngModule;if(o=Nf(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)hl(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;wl(o.imports,d=>{hl(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&ry(l,n)}if(!a){let l=xr(r)||(()=>new r);n({provide:r,useFactory:l,deps:yt},r),n({provide:eh,useValue:r,multi:!0},r),n({provide:Tr,useValue:()=>M(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;nh(c,d=>{n(d,l)})}}else return!1;return r!==t&&t.providers!==void 0}function nh(t,n){for(let e of t)Gf(e)&&(e=e.\u0275providers),Array.isArray(e)?nh(e,n):n(e)}var dx=ve({provide:String,useValue:ve});function oy(t){return t!==null&&typeof t=="object"&&dx in t}function ux(t){return!!(t&&t.useExisting)}function fx(t){return!!(t&&t.useFactory)}function Ir(t){return typeof t=="function"}function sy(t){return!!t.useClass}var Ks=new _(""),ul={},zv={},Rf;function Ao(){return Rf===void 0&&(Rf=new Mo),Rf}var be=class{},Sr=class extends be{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Lf(n,s=>this.processProvider(s)),this.records.set(Zs,So(void 0,this)),r.has("environment")&&this.records.set(be,So(void 0,this));let o=this.records.get(Ks);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(eh,yt,{self:!0}))}retrieve(n,e){let i=Dr(e)||0;try{return this.get(n,wr,i)}catch(r){if(_o(r))return r;throw r}}destroy(){Us(this),this._destroyed=!0;let n=B(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),B(n)}}onDestroy(n){return Us(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Us(this);let e=Ln(this),i=Kt(void 0),r;try{return n()}finally{Ln(e),Kt(i)}}get(n,e=wr,i){if(Us(this),n.hasOwnProperty(Hv))return n[Hv](this);let r=Dr(i),o,s=Ln(this),a=Kt(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=vx(n)&&qs(n);d&&this.injectableDefInScope(d)?l=So(Pf(n),ul):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Ao():this.parent;return e=r&8&&e===wr?null:e,c.get(n,e)}catch(c){let l=ix(c);throw l===-200||l===-201?new E(l,null):c}finally{Kt(a),Ln(s)}}resolveInjectorInitializers(){let n=B(null),e=Ln(this),i=Kt(void 0),r;try{let o=this.get(Tr,yt,{self:!0});for(let s of o)s()}finally{Ln(e),Kt(i),B(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=dt(n);let e=Ir(n)?n:dt(n&&n.provide),i=px(n);if(!Ir(n)&&n.multi===!0){let r=this.records.get(e);r||(r=So(void 0,ul,!0),r.factory=()=>Ff(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=B(null);try{if(e.value===zv)throw Qf("");return e.value===ul&&(e.value=zv,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&gx(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{B(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=dt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Pf(t){let n=qs(t),e=n!==null?n.factory:xr(t);if(e!==null)return e;if(t instanceof _)throw new E(-204,!1);if(t instanceof Function)return hx(t);throw new E(-204,!1)}function hx(t){if(t.length>0)throw new E(-204,!1);let e=ex(t);return e!==null?()=>e.factory(t):()=>new t}function px(t){if(oy(t))return So(void 0,t.useValue);{let n=ih(t);return So(n,ul)}}function ih(t,n,e){let i;if(Ir(t)){let r=dt(t);return xr(r)||Pf(r)}else if(oy(t))i=()=>dt(t.useValue);else if(fx(t))i=()=>t.useFactory(...Ff(t.deps||[]));else if(ux(t))i=(r,o)=>M(dt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=dt(t&&(t.useClass||t.provide));if(mx(t))i=()=>new r(...Ff(t.deps));else return xr(r)||Pf(r)}return i}function Us(t){if(t.destroyed)throw new E(-205,!1)}function So(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function mx(t){return!!t.deps}function gx(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function vx(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Lf(t,n){for(let e of t)Array.isArray(e)?Lf(e,n):e&&Gf(e)?Lf(e.\u0275providers,n):n(e)}function st(t,n){let e;t instanceof Sr?(Us(t),e=t):e=new Of(t);let i,r=Ln(e),o=Kt(void 0);try{return n()}finally{Ln(r),Kt(o)}}function ay(){return Qv()!==void 0||Nc()!=null}var Sn=0,N=1,U=2,ot=3,ln=4,_t=5,Ro=6,No=7,ut=8,fi=9,Mn=10,De=11,ko=12,rh=13,Li=14,Ft=15,Vi=16,Ar=17,Un=18,hi=19,oh=20,ci=21,Il=22,ki=23,Qt=24,Rr=25,ji=26,ze=27,cy=1;var Bi=7,Qs=8,Nr=9,ft=10;function pi(t){return Array.isArray(t)&&typeof t[cy]=="object"}function dn(t){return Array.isArray(t)&&t[cy]===!0}function sh(t){return(t.flags&4)!==0}function mi(t){return t.componentOffset>-1}function Oo(t){return(t.flags&1)===1}function Hn(t){return!!t.template}function Fo(t){return(t[U]&512)!==0}function kr(t){return(t[U]&256)===256}var ah="svg",ly="math";function un(t){for(;Array.isArray(t);)t=t[Sn];return t}function ch(t,n){return un(n[t])}function fn(t,n){return un(n[t.index])}function Sl(t,n){return t.data[n]}function dy(t,n){return t[n]}function hn(t,n){let e=n[t];return pi(e)?e:e[Sn]}function uy(t){return(t[U]&4)===4}function Ml(t){return(t[U]&128)===128}function fy(t){return dn(t[ot])}function zn(t,n){return n==null?null:t[n]}function lh(t){t[Ar]=0}function dh(t){t[U]&1024||(t[U]|=1024,Ml(t)&&Or(t))}function hy(t,n){for(;t>0;)n=n[Li],t--;return n}function Xs(t){return!!(t[U]&9216||t[Qt]?.dirty)}function Tl(t){t[Mn].changeDetectionScheduler?.notify(8),t[U]&64&&(t[U]|=1024),Xs(t)&&Or(t)}function Or(t){t[Mn].changeDetectionScheduler?.notify(0);let n=li(t);for(;n!==null&&!(n[U]&8192||(n[U]|=8192,!Ml(n)));)n=li(n)}function Al(t,n){if(kr(t))throw new E(911,!1);t[ci]===null&&(t[ci]=[]),t[ci].push(n)}function py(t,n){if(t[ci]===null)return;let e=t[ci].indexOf(n);e!==-1&&t[ci].splice(e,1)}function li(t){let n=t[ot];return dn(n)?n[ot]:n}function uh(t){return t[No]??=[]}function fh(t){return t.cleanup??=[]}function my(t,n,e,i){let r=uh(n);r.push(e),t.firstCreatePass&&fh(t).push(i,r.length-1)}var ne={lFrame:Sy(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Vf=!1;function gy(){return ne.lFrame.elementDepthCount}function vy(){ne.lFrame.elementDepthCount++}function hh(){ne.lFrame.elementDepthCount--}function Rl(){return ne.bindingsEnabled}function ph(){return ne.skipHydrationRootTNode!==null}function mh(t){return ne.skipHydrationRootTNode===t}function gh(){ne.skipHydrationRootTNode=null}function q(){return ne.lFrame.lView}function Re(){return ne.lFrame.tView}function xe(t){return ne.lFrame.contextLView=t,t[ut]}function Ie(t){return ne.lFrame.contextLView=null,t}function We(){let t=vh();for(;t!==null&&t.type===64;)t=t.parent;return t}function vh(){return ne.lFrame.currentTNode}function yy(){let t=ne.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Po(t,n){let e=ne.lFrame;e.currentTNode=t,e.isParent=n}function yh(){return ne.lFrame.isParent}function _h(){ne.lFrame.isParent=!1}function _y(){return ne.lFrame.contextLView}function bh(){return Vf}function zs(t){let n=Vf;return Vf=t,n}function by(){let t=ne.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Cy(t){return ne.lFrame.bindingIndex=t}function Fr(){return ne.lFrame.bindingIndex++}function Ch(t){let n=ne.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Ey(){return ne.lFrame.inI18n}function wy(t,n){let e=ne.lFrame;e.bindingIndex=e.bindingRootIndex=t,Nl(n)}function Dy(){return ne.lFrame.currentDirectiveIndex}function Nl(t){ne.lFrame.currentDirectiveIndex=t}function xy(t){let n=ne.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function kl(){return ne.lFrame.currentQueryIndex}function Js(t){ne.lFrame.currentQueryIndex=t}function yx(t){let n=t[N];return n.type===2?n.declTNode:n.type===1?t[_t]:null}function Eh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=yx(o),r===null||(o=o[Li],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ne.lFrame=Iy();return i.currentTNode=n,i.lView=t,!0}function Ol(t){let n=Iy(),e=t[N];ne.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Iy(){let t=ne.lFrame,n=t===null?null:t.child;return n===null?Sy(t):n}function Sy(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function My(){let t=ne.lFrame;return ne.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var wh=My;function Fl(){let t=My();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Ty(t){return(ne.lFrame.contextLView=hy(t,ne.lFrame.contextLView))[ut]}function Ui(){return ne.lFrame.selectedIndex}function Hi(t){ne.lFrame.selectedIndex=t}function Lo(){let t=ne.lFrame;return Sl(t.tView,t.selectedIndex)}function zi(){ne.lFrame.currentNamespace=ah}function Pl(){_x()}function _x(){ne.lFrame.currentNamespace=null}function Dh(){return ne.lFrame.currentNamespace}var Ay=!0;function Ll(){return Ay}function ea(t){Ay=t}function jf(t,n=null,e=null,i){let r=xh(t,n,e,i);return r.resolveInjectorInitializers(),r}function xh(t,n=null,e=null,i,r=new Set){let o=[e||yt,iy(t)],s;return new Sr(o,n||Ao(),s||null,r)}var ce=class t{static THROW_IF_NOT_FOUND=wr;static NULL=new Mo;static create(n,e){if(Array.isArray(n))return jf({name:""},e,n,"");{let i=n.name??"";return jf({name:i},n.parent,n.providers,i)}}static \u0275prov=F({token:t,providedIn:"any",factory:()=>M(Zs)});static __NG_ELEMENT_ID__=-1},V=new _(""),ht=(()=>{class t{static __NG_ELEMENT_ID__=bx;static __NG_ENV_ID__=e=>e}return t})(),pl=class extends ht{_lView;constructor(n){super(),this._lView=n}get destroyed(){return kr(this._lView)}onDestroy(n){let e=this._lView;return Al(e,n),()=>py(e,n)}};function bx(){return new pl(q())}var Ry=!1,Ny=new _(""),gi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new et(!1);debugTaskTracker=u(Ny,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ee(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})(),Bf=class extends I{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,ay()&&(this.destroyRef=u(ht,{optional:!0})??void 0,this.pendingTasks=u(gi,{optional:!0})??void 0)}emit(n){let e=B(null);try{super.next(n)}finally{B(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof de&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},X=Bf;function ml(...t){}function Ih(t){let n,e;function i(){t=ml;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch(r){}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function ky(t){return queueMicrotask(()=>t()),()=>{t=ml}}var Sh="isAngularZone",$s=Sh+"_ID",Cx=0,k=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new X(!1);onMicrotaskEmpty=new X(!1);onStable=new X(!1);onError=new X(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Ry}=n;if(typeof Zone>"u")throw new E(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,Dx(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Sh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new E(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new E(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,Ex,ml,ml);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},Ex={};function Mh(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function wx(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Ih(()=>{t.callbackScheduled=!1,Uf(t),t.isCheckStableRunning=!0,Mh(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Uf(t)}function Dx(t){let n=()=>{wx(t)},e=Cx++;t._inner=t._inner.fork({name:"angular",properties:{[Sh]:!0,[$s]:e,[$s+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(xx(c))return i.invokeTask(o,s,a,c);try{return $v(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Gv(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return $v(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!Ix(c)&&n(),Gv(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Uf(t),Mh(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Uf(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function $v(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Gv(t){t._nesting--,Mh(t)}var Gs=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new X;onMicrotaskEmpty=new X;onStable=new X;onError=new X;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function xx(t){return Oy(t,"__ignore_ng_zone__")}function Ix(t){return Oy(t,"__scheduler_tick__")}function Oy(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Mt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Xt=new _("",{factory:()=>{let t=u(k),n=u(be),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Mt),e.handleError(i))})}}}),Fy={provide:Tr,useValue:()=>{let t=u(Mt,{optional:!0})},multi:!0},Sx=new _("",{factory:()=>{let t=u(V).defaultView;if(!t)return;let n=u(Xt),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(ht).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Th(){return ui([ny(()=>{u(Sx)})])}function ie(t,n){let[e,i,r]=ff(t,n?.equal),o=e,s=o[Be];return o.set=i,o.update=r,o.asReadonly=Vl.bind(o),o}function Vl(){let t=this[Be];if(t.readonlyFn===void 0){let n=()=>this();n[Be]=t,t.readonlyFn=n}return t.readonlyFn}var Pr=new _("",{factory:()=>Mx}),Mx="ng";var jl=new _(""),Lr=new _("",{providedIn:"platform",factory:()=>"unknown"}),ta=new _(""),Vr=new _("",{factory:()=>u(V).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Vo=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Tx}return t})();function Tx(){return new Vo(q(),We())}var Bn=class{},na=new _("",{factory:()=>!0});var Ah=new _(""),Bl=(()=>{class t{static \u0275prov=F({token:t,providedIn:"root",factory:()=>new Hf})}return t})(),Hf=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},gl=class{[Be];constructor(n){this[Be]=n}destroy(){this[Be].destroy()}};function Tn(t,n){let e=n?.injector??u(ce),i=n?.manualCleanup!==!0?e.get(ht):null,r,o=e.get(Vo,null,{optional:!0}),s=e.get(Bn);return o!==null?(r=Nx(o.view,s,t),i instanceof pl&&i._lView===o.view&&(i=null)):r=kx(t,e.get(Bl),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new gl(r)}var Py=j(C({},hf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=zs(!1);try{pf(this)}finally{zs(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=B(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],B(t)}}}),Ax=j(C({},Py),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Ni(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Rx=j(C({},Py),{consumerMarkedDirty(){this.view[U]|=8192,Or(this.view),this.notifier.notify(13)},destroy(){if(Ni(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[ki]?.delete(this)}});function Nx(t,n,e){let i=Object.create(Rx);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=Ly(i,e),t[ki]??=new Set,t[ki].add(i),i.consumerMarkedDirty(i),i}function kx(t,n,e){let i=Object.create(Ax);return i.fn=Ly(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Ly(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function pn(t){return typeof t=="function"&&t[Be]!==void 0}function Ul(t){return pn(t)&&typeof t.set=="function"}var ia=(()=>{class t{internalPendingTasks=u(gi);scheduler=u(Bn);errorHandler=u(Xt);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})();function fa(t){return{toString:t}.toString()}var he=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(he||{}),Kl=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function E_(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var w_=null,Ye=(()=>{w_=Vy;let t=()=>Vy;return t.ngInherit=!0,t})();function zx(){return w_}function Vy(t){return t.type.prototype.ngOnChanges&&(t.setInput=Gx),$x}function $x(){let t=D_(this),n=t?.current;if(n){let e=t.previous;if(e===Pi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function Gx(t,n,e,i,r){let o=this.declaredInputs[i],s=D_(t)||Wx(t,{previous:Pi,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Kl(l&&l.currentValue,e,c===Pi),E_(t,n,r,e)}var Bh="__ngSimpleChanges__";function D_(t){return Object.hasOwn(t,Bh)&&t[Bh]||null}function Wx(t,n){return t[Bh]=n}var jy=[];var ye=function(t,n=null,e){for(let i=0;i<jy.length;i++){let r=jy[i];r(t,n,e)}};function qx(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=zx()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function x_(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function Wl(t,n,e){I_(t,n,3,e)}function ql(t,n,e,i){(t[U]&3)===e&&I_(t,n,e,i)}function Rh(t,n){let e=t[U];(e&3)===n&&(e&=16383,e+=1,t[U]=e)}function I_(t,n,e,i){let r=i!==void 0?t[Ar]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[Ar]+=65536),(a<o||o==-1)&&(Yx(t,e,n,c),t[Ar]=(t[Ar]&4294901760)+c+2),c++}function By(t,n){ye(he.LifecycleHookStart,t,n);let e=B(null);try{n.call(t)}finally{B(e),ye(he.LifecycleHookEnd,t,n)}}function Yx(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[U]>>14<t[Ar]>>16&&(t[U]&3)===n&&(t[U]+=16384,By(a,o)):By(a,o)}var Bo=-1,jr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function Zx(t){return(t.flags&8)!==0}function Kx(t){return(t.flags&16)!==0}function Qx(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];Xx(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function S_(t){return t===3||t===4||t===6}function Xx(t){return t.charCodeAt(0)===64}function Ho(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?Uy(t,e,r,null,n[++i]):Uy(t,e,r,null,null))}}return t}function Uy(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function M_(t){return t!==Bo}function Ql(t){return t&32767}function Jx(t){return t>>16}function Xl(t,n){let e=Jx(t),i=n;for(;e>0;)i=i[Li],e--;return i}var Uh=!0;function Hy(t){let n=Uh;return Uh=t,n}var eI=256,T_=eI-1,A_=5,tI=0,$n={};function nI(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Mr)&&(i=e[Mr]),i==null&&(i=e[Mr]=tI++);let r=i&T_,o=1<<r;n.data[t+(r>>A_)]|=o}function Jl(t,n){let e=R_(t,n);if(e!==-1)return e;let i=n[N];i.firstCreatePass&&(t.injectorIndex=n.length,Nh(i.data,t),Nh(n,null),Nh(i.blueprint,null));let r=gp(t,n),o=t.injectorIndex;if(M_(r)){let s=Ql(r),a=Xl(r,n),c=a[N].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function Nh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function R_(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function gp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=P_(r),i===null)return Bo;if(e++,r=r[Li],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Bo}function Hh(t,n,e){nI(t,n,e)}function iI(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(S_(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function N_(t,n,e){if(e&8||t!==void 0)return t;El(n,"NodeInjector")}function k_(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[fi],o=Kt(void 0);try{return r?r.get(n,i,e&8):Xf(n,i,e&8)}finally{Kt(o)}}return N_(i,n,e)}function O_(t,n,e,i=0,r){if(t!==null){if(n[U]&2048&&!(i&2)){let s=aI(t,n,e,i,$n);if(s!==$n)return s}let o=F_(t,n,e,i,$n);if(o!==$n)return o}return k_(n,e,i,r)}function F_(t,n,e,i,r){let o=oI(e);if(typeof o=="function"){if(!Eh(n,t,i))return i&1?N_(r,e,i):k_(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))El(e);else return s}finally{wh()}}else if(typeof o=="number"){let s=null,a=R_(t,n),c=Bo,l=i&1?n[Ft][_t]:null;for((a===-1||i&4)&&(c=a===-1?gp(t,n):n[a+8],c===Bo||!$y(i,!1)?a=-1:(s=n[N],a=Ql(c),n=Xl(c,n)));a!==-1;){let d=n[N];if(zy(o,a,d.data)){let f=rI(a,n,e,s,i,l);if(f!==$n)return f}c=n[a+8],c!==Bo&&$y(i,n[N].data[a+8]===l)&&zy(o,a,n)?(s=d,a=Ql(c),n=Xl(c,n)):a=-1}}return r}function rI(t,n,e,i,r,o){let s=n[N],a=s.data[t+8],c=i==null?mi(a)&&Uh:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Yl(a,s,e,c,l);return d!==null?aa(n,s,d,a,r):$n}function Yl(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,d=o>>20,f=i?a:a+d,h=r?a+d:l;for(let g=f;g<h;g++){let b=s[g];if(g<c&&e===b||g>=c&&b.type===e)return g}if(r){let g=s[c];if(g&&Hn(g)&&g.type===e)return c}return null}function aa(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof jr){let a=o;if(a.resolving)throw Qf("");let c=Hy(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,f=a.injectImpl?Kt(a.injectImpl):null,h=Eh(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&qx(e,s[e],n)}finally{f!==null&&Kt(f),Hy(c),a.resolving=!1,wh()}}return o}function oI(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Mr)?t[Mr]:void 0;return typeof n=="number"?n>=0?n&T_:sI:n}function zy(t,n,e){let i=1<<t;return!!(e[n+(t>>A_)]&i)}function $y(t,n){return!(t&2)&&!(t&1&&n)}var $i=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return O_(this._tNode,this._lView,n,Dr(i),e)}};function sI(){return new $i(We(),q())}function Wn(t){return fa(()=>{let n=t.prototype.constructor,e=n[Hs]||zh(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Hs]||zh(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function zh(t){return zf(t)?()=>{let n=zh(dt(t));return n&&n()}:xr(t)}function aI(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[U]&2048&&!Fo(s);){let a=F_(o,s,e,i|2,$n);if(a!==$n)return a;let c=o.parent;if(!c){let l=s[oh];if(l){let d=l.get(e,$n,i&-5);if(d!==$n)return d}c=P_(s),s=s[Li]}o=c}return r}function P_(t){let n=t[N],e=n.type;return e===2?n.declTNode:e===1?t[_t]:null}function ha(t){return iI(We(),t)}function T(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function cI(){return Wo(We(),q())}function Wo(t,n){return new H(fn(t,n))}var H=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=cI}return t})();function L_(t){return t instanceof H?t.nativeElement:t}function lI(){return this._results[Symbol.iterator]()}var vi=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new I}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Jv(n);(this._changesDetected=!Xv(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=lI};function V_(t){return(t.flags&128)===128}var vp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(vp||{}),j_=new Map,dI=0;function uI(){return dI++}function fI(t){j_.set(t[hi],t)}function $h(t){j_.delete(t[hi])}var Gy="__ngContext__";function zo(t,n){pi(n)?(t[Gy]=n[hi],fI(n)):t[Gy]=n}function B_(t){return H_(t[ko])}function U_(t){return H_(t[ln])}function H_(t){for(;t!==null&&!dn(t);)t=t[ln];return t}var Gh;function yp(t){Gh=t}function z_(){if(Gh!==void 0)return Gh;if(typeof document<"u")return document;throw new E(210,!1)}var $_=!1,G_=new _("",{factory:()=>$_});var Wy=new WeakMap;function hI(t,n){if(t==null||typeof t!="object")return;let e=Wy.get(t);e||(e=new WeakSet,Wy.set(t,e)),e.add(n)}var pI=(t,n,e,i)=>{};function mI(t,n,e,i){pI(t,n,e,i)}function fd(t){return(t.flags&32)===32}var gI=()=>null;function W_(t,n,e=!1){return gI(t,n,e)}function q_(t,n){let e=t.contentQueries;if(e!==null){let i=B(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];Js(o),a.contentQueries(2,n[s],s)}}}finally{B(i)}}}function Wh(t,n,e){Js(0);let i=B(null);try{n(t,e)}finally{B(i)}}function _p(t,n,e){if(sh(n)){let i=B(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{B(i)}}}var Nn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Nn||{});var Hl;function vI(){if(Hl===void 0&&(Hl=null,Fi.trustedTypes))try{Hl=Fi.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return Hl}function hd(t){return vI()?.createHTML(t)||t}var zl;function yI(){if(zl===void 0&&(zl=null,Fi.trustedTypes))try{zl=Fi.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return zl}function qy(t){return yI()?.createScriptURL(t)||t}var yi=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${vl})`}},qh=class extends yi{getTypeName(){return"HTML"}},Yh=class extends yi{getTypeName(){return"Style"}},Zh=class extends yi{getTypeName(){return"Script"}},Kh=class extends yi{getTypeName(){return"URL"}},Qh=class extends yi{getTypeName(){return"ResourceURL"}};function kn(t){return t instanceof yi?t.changingThisBreaksApplicationSecurity:t}function bi(t,n){let e=Y_(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${vl})`)}return e===n}function Y_(t){return t instanceof yi&&t.getTypeName()||null}function bp(t){return new qh(t)}function Cp(t){return new Yh(t)}function Ep(t){return new Zh(t)}function wp(t){return new Kh(t)}function Dp(t){return new Qh(t)}function _I(t){let n=new Jh(t);return bI()?new Xh(n):n}var Xh=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(hd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch(e){return null}}},Jh=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=hd(n),e}};function bI(){try{return!!new window.DOMParser().parseFromString(hd(""),"text/html")}catch(t){return!1}}var CI=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function pa(t){return t=String(t),t.match(CI)?t:"unsafe:"+t}function Ci(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function ma(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var Z_=Ci("area,br,col,hr,img,wbr"),K_=Ci("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Q_=Ci("rp,rt"),EI=ma(Q_,K_),wI=ma(K_,Ci("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),DI=ma(Q_,Ci("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Yy=ma(Z_,wI,DI,EI),X_=Ci("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),xI=Ci("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),II=Ci("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),SI=ma(X_,xI,II),MI=Ci("script,style,template"),ep=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=RI(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=AI(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=Zy(n).toLowerCase();if(!Yy.hasOwnProperty(e))return this.sanitizedSomething=!0,!MI.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!SI.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;X_[a]&&(c=pa(c)),this.buf.push(" ",s,'="',Ky(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=Zy(n).toLowerCase();Yy.hasOwnProperty(e)&&!Z_.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(Ky(n))}};function TI(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function AI(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw J_(n);return n}function RI(t){let n=t.firstChild;if(n&&TI(t,n))throw J_(n);return n}function Zy(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function J_(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var NI=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,kI=/([^\#-~ |!])/g;function Ky(t){return t.replace(/&/g,"&amp;").replace(NI,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(kI,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var $l;function xp(t,n){let e=null;try{$l=$l||_I(t);let i=n?String(n):"";e=$l.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=$l.getInertBodyElement(i)}while(i!==o);let a=new ep().sanitizeChildren(Qy(e)||e);return hd(a)}finally{if(e){let i=Qy(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Qy(t){return"content"in t&&OI(t)?t.content:null}function OI(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var FI=/^>|^->|<!--|-->|--!>|<!-$/g,PI=/(<|>)/g,LI="\u200B$1\u200B";function VI(t){return t.replace(FI,n=>n.replace(PI,LI))}function jI(t,n){return t.createText(n)}function BI(t,n,e){t.setValue(n,e)}function UI(t,n){return t.createComment(VI(n))}function eb(t,n,e){return t.createElement(n,e)}function ed(t,n,e,i,r){t.insertBefore(n,e,i,r)}function tb(t,n,e){t.appendChild(n,e)}function Xy(t,n,e,i,r){i!==null?ed(t,n,e,i,r):tb(t,n,e)}function HI(t,n,e,i){t.removeChild(null,n,e,i)}function zI(t,n,e){t.setAttribute(n,"style",e)}function $I(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function nb(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&Qx(t,n,i),r!==null&&$I(t,n,r),o!==null&&zI(t,n,o)}var nt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(nt||{});function ib(t){let n=ob();return n?n.sanitize(nt.URL,t)||"":bi(t,"URL")?kn(t):pa(Cl(t))}function rb(t){let n=ob();if(n)return qy(n.sanitize(nt.RESOURCE_URL,t)||"");if(bi(t,"ResourceURL"))return qy(kn(t));throw new E(904,!1)}var GI={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function WI(t,n){return GI[t.toLowerCase()]?.[n.toLowerCase()]===!0?rb:ib}function Ip(t,n,e){return WI(n,e)(t)}function ob(){let t=q();return t&&t[Mn].sanitizer}function qI(t){return t instanceof Function?t():t}function YI(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var sb="ng-template";function ZI(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&YI(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Sp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Sp(t){return t.type===4&&t.value!==sb}function KI(t,n,e){let i=t.type===4&&!e?sb:t.value;return n===i}function QI(t,n,e){let i=4,r=t.attrs,o=r!==null?eS(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!An(i)&&!An(c))return!1;if(s&&An(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!KI(t,c,e)||c===""&&n.length===1){if(An(i))return!1;s=!0}}else if(i&8){if(r===null||!ZI(t,r,c,e)){if(An(i))return!1;s=!0}}else{let l=n[++a],d=XI(c,r,Sp(t),e);if(d===-1){if(An(i))return!1;s=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(An(i))return!1;s=!0}}}}return An(i)||s}function An(t){return(t&1)===0}function XI(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return tS(n,t)}function ab(t,n,e=!1){for(let i=0;i<n.length;i++)if(QI(t,n[i],e))return!0;return!1}function JI(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function eS(t){for(let n=0;n<t.length;n++){let e=t[n];if(S_(e))return n}return t.length}function tS(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function nS(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function Jy(t,n){return t?":not("+n.trim()+")":n}function iS(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!An(s)&&(n+=Jy(o,r),r=""),i=s,o=o||!An(i);e++}return r!==""&&(n+=Jy(o,r)),n}function rS(t){return t.map(iS).join(",")}function oS(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!An(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var gn={},Gn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Gn||{}),sS;function Mp(t,n){return sS(t,n)}var PH=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var tp=new WeakMap;function cb(t){return t?t[Li]??t:null}var ra=new WeakSet;function aS(t,n,e){let i=tp.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=cb(e);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],d=c.parentNode;c===n?(i.splice(a,1),ra.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function cS(t,n,e){let i=cb(e),r=tp.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):tp.set(t,[{el:n,declarationView:i}])}var Br=new Set,pd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(pd||{}),qn=new _(""),e_=new Set;function Wi(t){e_.has(t)||(e_.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var md=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})(),Tp=[0,1,2,3],Ap=(()=>{class t{ngZone=u(k);scheduler=u(Bn);errorHandler=u(Mt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(qn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&ye(he.AfterRenderHooksStart),this.executing=!0;for(let i of Tp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&ye(he.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Rr]??=[]).push(e),Or(i),i[U]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(pd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})(),ca=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Rr];n&&(this.view[Rr]=n.filter(e=>e!==this))}};function Yn(t,n){let e=n?.injector??u(ce);return Wi("NgAfterNextRender"),dS(t,e,n,!0)}function lS(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function dS(t,n,e,i){let r=n.get(md);r.impl??=n.get(Ap);let o=n.get(qn,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(ht):null,a=n.get(Vo,null,{optional:!0}),c=new ca(r.impl,lS(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var lb=new _("",{factory:()=>{let t=u(be),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function db(t,n,e){let i=t.get(lb);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function uS(t,n){let e=t.get(lb);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function fS(t,n){for(let[e,i]of n)db(t,i.animateFns)}function t_(t,n,e,i){let r=t?.[ji]?.enter;n!==null&&r&&r.has(e.index)&&fS(i,r)}function n_(t,n,e,i){try{e.get(Zs)}catch(s){return i(!1)}let r=t?.[ji];r?.enter?.has(n.index)&&uS(e,r.enter.get(n.index).animateFns);let o=hS(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];gd(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&Br.add(t[hi]),db(e,()=>pS(t,n,r||void 0,o,i),r||void 0)}function hS(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let c=t[N].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function pS(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(t&&gd(t,n,o),o.length>0){let s=e||t?.[ji];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),gS(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&Br.delete(t[hi]),r(!0)})}else t&&Br.delete(t[hi]),r(!1)}function gd(t,n,e){if(n.type&12){let r=t[n.index];if(dn(r))for(let o=ft;o<r.length;o++){let s=r[o];s[N].type===2&&mS(s,e)}}let i=n.child;for(;i;)gd(t,i,e),i=i.next}function mS(t,n){let e=t[ji];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[N].firstChild;for(;i;)gd(t,i,n),i=i.next}function gS(t,n,e){n.then(()=>{t[ji]?.running===n&&(t[ji].running=void 0,Br.delete(t[hi])),e(!0)})}function jo(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;dn(r)?c=r:pi(r)&&(l=!0,r=r[Sn]);let d=un(r);t===0&&i!==null?(t_(a,i,o,e),s==null?tb(n,i,d):ed(n,i,d,s||null,!0)):t===1&&i!==null?(t_(a,i,o,e),ed(n,i,d,s||null,!0),aS(o,d,a)):t===2?(a?.[ji]?.leave?.has(o.index)&&cS(o,d,a),ra.delete(d),n_(a,o,e,f=>{if(ra.has(d)){ra.delete(d);return}HI(n,d,l,f)})):t===3&&(ra.delete(d),n_(a,o,e,()=>{n.destroyNode(d)})),c!=null&&IS(n,t,e,c,o,i,s)}}function vS(t,n){ub(t,n),n[Sn]=null,n[_t]=null}function yS(t,n,e,i,r,o){i[Sn]=r,i[_t]=n,vd(t,i,e,1,r,o)}function ub(t,n){n[Mn].changeDetectionScheduler?.notify(9),vd(t,n,n[De],2,null,null)}function _S(t){let n=t[ko];if(!n)return kh(t[N],t);for(;n;){let e=null;if(pi(n))e=n[ko];else{let i=n[ft];i&&(e=i)}if(!e){for(;n&&!n[ln]&&n!==t;)pi(n)&&kh(n[N],n),n=n[ot];n===null&&(n=t),pi(n)&&kh(n[N],n),e=n&&n[ln]}n=e}}function Rp(t,n){let e=t[Nr],i=e.indexOf(n);e.splice(i,1)}function Np(t,n){if(kr(n))return;let e=n[De];e.destroyNode&&vd(t,n,e,3,null,null),_S(n)}function kh(t,n){if(kr(n))return;let e=B(null);try{n[U]&=-129,n[U]|=256,n[Qt]&&Ni(n[Qt]),CS(t,n),bS(t,n),n[N].type===1&&n[De].destroy();let i=n[Vi];if(i!==null&&dn(n[ot])){i!==n[ot]&&Rp(i,n);let r=n[Un];r!==null&&r.detachView(t)}$h(n)}finally{B(e)}}function bS(t,n){let e=t.cleanup,i=n[No];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[No]=null);let r=n[ci];if(r!==null){n[ci]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[ki];if(o!==null){n[ki]=null;for(let s of o)s.destroy()}}function CS(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof jr)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];ye(he.LifecycleHookStart,a,c);try{c.call(a)}finally{ye(he.LifecycleHookEnd,a,c)}}else{ye(he.LifecycleHookStart,r,o);try{o.call(r)}finally{ye(he.LifecycleHookEnd,r,o)}}}}}function fb(t,n,e){return ES(t,n.parent,e)}function ES(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Sn];if(mi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Nn.None||r===Nn.Emulated)return null}return fn(i,e)}function hb(t,n,e){return DS(t,n,e)}function wS(t,n,e){return t.type&40?fn(t,e):null}var DS=wS,i_;function kp(t,n,e,i){let r=fb(t,i,n),o=n[De],s=i.parent||n[_t],a=hb(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)Xy(o,r,e[c],a,!1);else Xy(o,r,e,a,!1);i_!==void 0&&i_(o,i,n,e,r)}function oa(t,n){if(n!==null){let e=n.type;if(e&3)return fn(n,t);if(e&4)return np(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return oa(t,i);{let r=t[n.index];return dn(r)?np(-1,r):un(r)}}else{if(e&128)return oa(t,n.next);if(e&32)return Mp(n,t)()||un(t[n.index]);{let i=pb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=li(t[Ft]);return oa(r,i)}else return oa(t,n.next)}}}return null}function pb(t,n){if(n!==null){let i=t[Ft][_t],r=n.projection;return i.projection[r]}return null}function np(t,n){let e=ft+t+1;if(e<n.length){let i=n[e],r=i[N].firstChild;if(r!==null)return oa(i,r)}return n[Bi]}function Op(t,n,e,i,r,o,s){for(;e!=null;){let a=i[fi];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&zo(un(c),i),e.flags|=2),!fd(e))if(l&8)Op(t,n,e.child,i,r,o,!1),jo(n,t,a,r,c,e,o,i);else if(l&32){let d=Mp(e,i),f;for(;f=d();)jo(n,t,a,r,f,e,o,i);jo(n,t,a,r,c,e,o,i)}else l&16?mb(t,n,i,e,r,o):jo(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function vd(t,n,e,i,r,o){Op(e,i,t.firstChild,n,r,o,!1)}function xS(t,n,e){let i=n[De],r=fb(t,e,n),o=e.parent||n[_t],s=hb(o,e,n);mb(i,0,n,e,r,s)}function mb(t,n,e,i,r,o){let s=e[Ft],c=s[_t].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];jo(n,t,e[fi],r,d,i,o,e)}else{let l=c,d=s[ot];V_(i)&&(l.flags|=128),Op(t,n,l,d,r,o,!0)}}function IS(t,n,e,i,r,o,s){let a=i[Bi],c=un(i);a!==c&&jo(n,t,e,o,a,r,s);for(let l=ft;l<i.length;l++){let d=i[l];vd(d[N],d,t,n,o,a)}}function SS(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Gn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Gn.Important),t.setStyle(e,i,r,o))}}function Fp(t,n,e,i,r,o,s,a,c,l,d){let f=ze+i,h=f+r,g=MS(f,h),b=typeof l=="function"?l():l;return g[N]={type:t,blueprint:g,template:e,queries:null,viewQuery:a,declTNode:n,data:g.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:b,incompleteFirstPass:!1,ssrId:d}}function MS(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:gn);return e}function TS(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Fp(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Pp(t,n,e,i,r,o,s,a,c,l,d){let f=n.blueprint.slice();return f[Sn]=r,f[U]=i|4|128|8|64|1024,(l!==null||t&&t[U]&2048)&&(f[U]|=2048),lh(f),f[ot]=f[Li]=t,f[ut]=e,f[Mn]=s||t&&t[Mn],f[De]=a||t&&t[De],f[fi]=c||t&&t[fi]||null,f[_t]=o,f[hi]=uI(),f[Ro]=d,f[oh]=l,f[Ft]=n.type==2?t[Ft]:f,f}function AS(t,n,e){let i=fn(n,t),r=TS(e),o=t[Mn].rendererFactory,s=Lp(t,Pp(t,r,null,gb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function gb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function vb(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Lp(t,n){return t[ko]?t[rh][ln]=n:t[ko]=n,t[rh]=n,n}function v(t=1){yb(Re(),q(),Ui()+t,!1)}function yb(t,n,e,i){if(!i)if((n[U]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Wl(n,o,e)}else{let o=t.preOrderHooks;o!==null&&ql(n,o,0,e)}Hi(e)}var yd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(yd||{});function Ur(t,n,e,i){let r=B(null);try{let[o,s,a]=t.inputs[e],c=null;(s&yd.SignalBased)!==0&&(c=n[o][Be]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):E_(n,c,o,i)}finally{B(r)}}function _b(t,n,e,i,r){let o=Ui(),s=i&2;try{Hi(-1),s&&n.length>ze&&yb(t,n,ze,!1);let a=s?he.TemplateUpdateStart:he.TemplateCreateStart;ye(a,r,e),e(i,r)}finally{Hi(o);let a=s?he.TemplateUpdateEnd:he.TemplateCreateEnd;ye(a,r,e)}}function _d(t,n,e){PS(t,n,e),(e.flags&64)===64&&LS(t,n,e)}function ga(t,n,e=fn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function RS(t,n,e,i){let o=i.get(G_,$_)||e===Nn.ShadowDom||e===Nn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return NS(s),s}function NS(t){kS(t)}var kS=()=>null;function OS(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function bb(t,n,e,i,r,o){let s=n[N];if(Up(t,s,n,e,i)){mi(t)&&FS(n,t.index);return}t.type&3&&(e=OS(e)),Cb(t,n,e,i,r,o)}function Cb(t,n,e,i,r,o){if(t.type&3){let s=fn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function FS(t,n){let e=hn(n,t);e[U]&16||(e[U]|=64)}function PS(t,n,e){let i=e.directiveStart,r=e.directiveEnd;mi(e)&&AS(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Jl(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=aa(n,t,s,e);if(zo(c,n),o!==null&&US(n,s-i,c,a,e,o),Hn(a)){let l=hn(e.index,n);l[ut]=aa(n,t,s,e)}}}function LS(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Dy();try{Hi(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Nl(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&VS(c,l)}}finally{Hi(-1),Nl(s)}}function VS(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Vp(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];ab(n,o.selectors,!1)&&(i??=[],Hn(o)?i.unshift(o):i.push(o))}return i}function jS(t,n,e,i,r,o){let s=fn(t,n);BS(n[De],s,o,t.value,e,i,r)}function BS(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Cl(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function US(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Ur(i,e,c,l)}}function jp(t,n,e,i,r){let o=ze+e,s=n[N],a=r(s,n,t,i,e);n[o]=a,Po(t,!0);let c=t.type===2;return c?(nb(n[De],a,t),(gy()===0||Oo(t))&&zo(a,n),vy()):zo(a,n),Ll()&&(!c||!fd(t))&&kp(s,n,a,t),t}function Bp(t){let n=t;return yh()?_h():(n=n.parent,Po(n,!1)),n}function HS(t,n){let e=t[fi];if(!e)return;let i;try{i=e.get(Xt,null)}catch(r){i=null}i?.(n)}function Up(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],f=n.data[l];Ur(f,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=n.data[c];Ur(d,l,i,r),a=!0}return a}function zS(t,n,e,i,r,o){let s=null,a=null,c=null,l=!1,d=t.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let f=t.hostDirectiveInputs[r];for(let h=0;h<f.length;h+=2){let g=f[h];if(g>=a&&g<=c){let b=n.data[g],w=f[h+1];Ur(b,e[g],w,o),l=!0}else if(g>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(Ur(i,e[s],r,o),l=!0),l}function $S(t,n){let e=hn(n,t),i=e[N];GS(i,e);let r=e[Sn];r!==null&&e[Ro]===null&&(e[Ro]=W_(r,e[fi])),ye(he.ComponentStart);try{Hp(i,e,e[ut])}finally{ye(he.ComponentEnd,e[ut])}}function GS(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Hp(t,n,e){Ol(n);try{let i=t.viewQuery;i!==null&&Wh(1,i,e);let r=t.template;r!==null&&_b(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Un]?.finishViewCreation(t),t.staticContentQueries&&q_(t,n),t.staticViewQueries&&Wh(2,t.viewQuery,e);let o=t.components;o!==null&&WS(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[U]&=-5,Fl()}}function WS(t,n){for(let e=0;e<n.length;e++)$S(t,n[e])}function zp(t,n,e,i){let r=B(null);try{let o=n.tView,a=t[U]&4096?4096:16,c=Pp(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[Vi]=l;let d=t[Un];return d!==null&&(c[Un]=d.createEmbeddedView(o)),Hp(o,c,e),c}finally{B(r)}}function td(t,n){return!n||n.firstChild===null||V_(t)}function la(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(un(o)),dn(o)&&Eb(o,i);let s=e.type;if(s&8)la(t,n,e.child,i);else if(s&32){let a=Mp(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=pb(n,e);if(Array.isArray(a))i.push(...a);else{let c=li(n[Ft]);la(c[N],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function Eb(t,n){for(let e=ft;e<t.length;e++){let i=t[e],r=i[N].firstChild;r!==null&&la(i[N],i,r,n)}t[Bi]!==t[Sn]&&n.push(t[Bi])}function wb(t){if(t[Rr]!==null){for(let n of t[Rr])n.impl.addSequence(n);t[Rr].length=0}}var Db=[];function qS(t){return t[Qt]??YS(t)}function YS(t){let n=Db.pop()??Object.create(KS);return n.lView=t,n}function ZS(t){t.lView[Qt]!==t&&(t.lView=null,Db.push(t))}var KS=j(C({},Ti),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Or(t.lView)},consumerOnSignalRead(){this.lView[Qt]=this}});function QS(t){let n=t[Qt]??Object.create(XS);return n.lView=t,n}var XS=j(C({},Ti),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=li(t.lView);for(;n&&!xb(n[N]);)n=li(n);n&&dh(n)},consumerOnSignalRead(){this.lView[Qt]=this}});function xb(t){return t.type!==2}function Ib(t){if(t[ki]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[ki])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[U]&8192)}}var JS=100;function Sb(t,n=0){let i=t[Mn].rendererFactory,r=!1;r||i.begin?.();try{eM(t,n)}finally{r||i.end?.()}}function eM(t,n){let e=bh();try{zs(!0),ip(t,n);let i=0;for(;Xs(t);){if(i===JS)throw new E(103,!1);i++,ip(t,1)}}finally{zs(e)}}function tM(t,n,e,i){if(kr(n))return;let r=n[U],o=!1,s=!1;Ol(n);let a=!0,c=null,l=null;o||(xb(t)?(l=qS(n),c=oi(l)):Ac()===null?(a=!1,l=QS(n),c=oi(l)):n[Qt]&&(Ni(n[Qt]),n[Qt]=null));try{lh(n),Cy(t.bindingStartIndex),e!==null&&_b(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let g=t.preOrderCheckHooks;g!==null&&Wl(n,g,null)}else{let g=t.preOrderHooks;g!==null&&ql(n,g,0,null),Rh(n,0)}if(s||nM(n),Ib(n),Mb(n,0),t.contentQueries!==null&&q_(t,n),!o)if(d){let g=t.contentCheckHooks;g!==null&&Wl(n,g)}else{let g=t.contentHooks;g!==null&&ql(n,g,1),Rh(n,1)}rM(t,n);let f=t.components;f!==null&&Ab(n,f,0);let h=t.viewQuery;if(h!==null&&Wh(2,h,i),!o)if(d){let g=t.viewCheckHooks;g!==null&&Wl(n,g)}else{let g=t.viewHooks;g!==null&&ql(n,g,2),Rh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Il]){for(let g of n[Il])g();n[Il]=null}o||(wb(n),n[U]&=-73)}catch(d){throw o||Or(n),d}finally{l!==null&&(Ri(l,c),a&&ZS(l)),Fl()}}function Mb(t,n){for(let e=B_(t);e!==null;e=U_(e))for(let i=ft;i<e.length;i++){let r=e[i];Tb(r,n)}}function nM(t){for(let n=B_(t);n!==null;n=U_(n)){if(!(n[U]&2))continue;let e=n[Nr];for(let i=0;i<e.length;i++){let r=e[i];dh(r)}}}function iM(t,n,e){ye(he.ComponentStart);let i=hn(n,t);try{Tb(i,e)}finally{ye(he.ComponentEnd,i[ut])}}function Tb(t,n){Ml(t)&&ip(t,n)}function ip(t,n){let i=t[N],r=t[U],o=t[Qt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&vo(o)),s||=!1,o&&(o.dirty=!1),t[U]&=-9217,s)tM(i,t,i.template,t[ut]);else if(r&8192){let a=B(null);try{Ib(t),Mb(t,1);let c=i.components;c!==null&&Ab(t,c,1),wb(t)}finally{B(a)}}}function Ab(t,n,e){for(let i=0;i<n.length;i++)iM(t,n[i],e)}function rM(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Hi(~r);else{let o=r,s=e[++i],a=e[++i];wy(s,o);let c=n[o];ye(he.HostBindingsUpdateStart,c);try{a(2,c)}finally{ye(he.HostBindingsUpdateEnd,c)}}}}finally{Hi(-1)}}function $p(t,n){let e=bh()?64:1088;for(t[Mn].changeDetectionScheduler?.notify(n);t;){t[U]|=e;let i=li(t);if(Fo(t)&&!i)return t;t=i}return null}function Rb(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function oM(t,n){let e=ft+n;if(e<t.length)return t[e]}function Gp(t,n,e,i=!0){let r=n[N];if(aM(r,n,t,e),i){let s=np(e,t),a=n[De],c=a.parentNode(t[Bi]);c!==null&&yS(r,t[_t],a,n,c,s)}let o=n[Ro];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function sM(t,n){let e=nd(t,n);return e!==void 0&&Np(e[N],e),e}function nd(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e];if(i){let r=i[Vi];r!==null&&r!==t&&Rp(r,i),n>0&&(t[e-1][ln]=i[ln]);let o=Ys(t,ft+n);vS(i[N],i);let s=o[Un];s!==null&&s.detachView(o[N]),i[ot]=null,i[ln]=null,i[U]&=-129}return i}function aM(t,n,e,i){let r=ft+i,o=e.length;i>0&&(e[r-1][ln]=n),i<o-ft?(n[ln]=e[r],Jf(e,ft+i,n)):(e.push(n),n[ln]=null),n[ot]=e;let s=n[Vi];s!==null&&e!==s&&Nb(s,n);let a=n[Un];a!==null&&a.insertView(t),Tl(n),n[U]|=128}function Nb(t,n){let e=t[Nr],i=n[ot];if(pi(i))t[U]|=2;else{let r=i[ot][Ft];n[Ft]!==r&&(t[U]|=2)}e===null?t[Nr]=[n]:e.push(n)}var Gi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[N];return la(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[ut]}set context(n){this._lView[ut]=n}get destroyed(){return kr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[ot];if(dn(n)){let e=n[Qs],i=e?e.indexOf(this):-1;i>-1&&(nd(n,i),Ys(e,i))}this._attachedToViewContainer=!1}Np(this._lView[N],this._lView)}onDestroy(n){Al(this._lView,n)}markForCheck(){$p(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[U]&=-129}reattach(){Tl(this._lView),this._lView[U]|=128}detectChanges(){this._lView[U]|=1024,Sb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new E(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Fo(this._lView),e=this._lView[Vi];e!==null&&!n&&Rp(e,this._lView),ub(this._lView[N],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new E(902,!1);this._appRef=n;let e=Fo(this._lView),i=this._lView[Vi];i!==null&&!e&&Nb(i,this._lView),Tl(this._lView)}};var mn=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=cM;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=zp(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Gi(o)}}return t})();function cM(){return bd(We(),q())}function bd(t,n){return t.type&4?new mn(n,t,Wo(t,n)):null}function qo(t,n,e,i,r){let o=t.data[n];if(o===null)o=lM(t,n,e,i,r),Ey()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=yy();o.injectorIndex=s===null?-1:s.injectorIndex}return Po(o,!0),o}function lM(t,n,e,i,r){let o=vh(),s=yh(),a=s?o:o&&o.parent,c=t.data[n]=uM(t,a,e,n,i,r);return dM(t,c,o,s),c}function dM(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function uM(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return ph()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Dh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var fM=()=>null,hM=()=>null;function rp(t,n){return fM(t,n)}function pM(t,n,e){return hM(t,n,e)}var kb=class{},qe=class{},$e=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>mM()}return t})();function mM(){let t=q(),n=We(),e=hn(n.index,t);return(pi(e)?e:t)[De]}var Ob=(()=>{class t{static \u0275prov=F({token:t,providedIn:"root",factory:()=>null})}return t})();function Fb(t){return t.debugInfo?.className||t.type.name||null}var Zl={},id=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Zl,i);return r!==Zl||e===Zl?r:this.parentInjector.get(n,e,i)}};function Wp(t){return Lb(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function Pb(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function Lb(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function gM(t,n,e){return t[n]=e}function Ei(t,n,e){if(e===gn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Uo(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&hI(r,o);let s=mi(t)?hn(t.index,n):n;$p(s,5);let a=n[ut],c=r_(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=r_(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function r_(t,n,e,i){let r=B(null);try{return ye(he.OutputStart,n,e),e(i)!==!1}catch(o){return HS(t,o),!1}finally{ye(he.OutputEnd,n,e),B(r)}}function Vb(t,n,e,i,r,o,s,a){let c=Oo(t),l=!1,d=null;if(!i&&c&&(d=yM(n,e,o,t.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let f=fn(t,e),h=i?i(f):f;mI(e,h,o,a),i||(a.__ngNativeEl__=f);let g=r.listen(h,o,a);if(!vM(o)){let b=i?w=>i(un(w[t.index])):t.index;jb(b,n,e,o,a,g,!1)}}return l}function vM(t){return t.startsWith("animation")||t.startsWith("transition")}function yM(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[No],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function jb(t,n,e,i,r,o,s){let a=n.firstCreatePass?fh(n):null,c=uh(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function o_(t,n,e,i,r){let o=null,s=null,a=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let d=t.hostDirectiveOutputs[i];for(let f=0;f<d.length;f+=2){let h=d[f];if(h>=s&&h<=a)c=!0,rd(t,n,h,d[f+1],i,r);else if(h>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,rd(t,n,o,i,i,r)),c}function rd(t,n,e,i,r,o){let s=n[e],a=n[N],l=a.data[e].outputs[i],f=s[l].subscribe(o);jb(t.index,a,n,r,o,f,!0)}function Pe(){_M()}function _M(){let t=q(),n=Re(),e=We();if(n.firstCreatePass&&CM(n,e),e.controlDirectiveIndex===-1)return;Wi("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new od(t,n,e))}function Le(){bM()}function bM(){let t=q(),n=Re(),e=Lo();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new od(t,n,e))}var od=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return fn(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];o_(this.tNode,this.lView,i,n,Uo(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];o_(this.tNode,this.lView,i,e,Uo(this.tNode,this.lView,n))}listenToDom(n,e){Vb(this.tNode,this.tView,this.lView,void 0,this.lView[De],n,e,Uo(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];Ur(a,c,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];Ur(l,d,c,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";zS(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=s_(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=s_(o.directive);s!==null&&i.push(...s)}}}return e}};function s_(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function CM(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}EM(t,n)}function EM(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(a_(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(a_(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[h,g,b]=f;if(c>=g&&c<=b)return n.flags|=r,n.customControlIndex=h,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function a_(t,n){return wM(t,n)&&DM(t,n+"Change")}function wM(t,n){return n in t.inputs}function DM(t,n){return n in t.outputs}var op=Symbol("BINDING");var zr=new _("");function sd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=yl(r,a);else if(o==2){let c=a,l=n[++s];i=yl(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function D(t,n=0){let e=q();if(e===null)return M(t,n);let i=We();return O_(i,e,dt(t),n)}function Bb(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}SM(t,n,e,a,o,c,l)}o!==null&&i!==null&&xM(e,i,o)}function xM(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new E(-301,!1);i.push(n[r],o)}}function IM(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function SM(t,n,e,i,r,o,s){let a=i.length,c=null;for(let h=0;h<a;h++){let g=i[h];c===null&&Hn(g)&&(c=g,IM(t,e,h)),Hh(Jl(e,n),t,g.type)}kM(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let g=i[h];g.providersResolver&&g.providersResolver(g)}let l=!1,d=!1,f=vb(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let g=i[h];if(e.mergedAttrs=Ho(e.mergedAttrs,g.hostAttrs),TM(t,e,n,f,g),NM(f,g,r),s!==null&&s.has(g)){let[w,R]=s.get(g);e.directiveToIndex.set(g.type,[f,w+e.directiveStart,R+e.directiveStart])}else(o===null||!o.has(g))&&e.directiveToIndex.set(g.type,f);g.contentQueries!==null&&(e.flags|=4),(g.hostBindings!==null||g.hostAttrs!==null||g.hostVars!==0)&&(e.flags|=64);let b=g.type.prototype;!l&&(b.ngOnChanges||b.ngOnInit||b.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(b.ngOnChanges||b.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),f++}MM(t,e,o)}function MM(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))c_(0,n,r,i),c_(1,n,r,i),d_(n,i,!1);else{let o=e.get(r);l_(0,n,o,i),l_(1,n,o,i),d_(n,i,!0)}}}function c_(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),Ub(n,o)}}function l_(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),Ub(n,s)}}function Ub(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function d_(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Sp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function TM(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=xr(r.type,!0)),s=new jr(o,Hn(r),D,null);t.blueprint[i]=s,e[i]=s,AM(t,n,i,vb(t,e,r.hostVars,gn),r)}function AM(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;RM(s)!=a&&s.push(a),s.push(e,i,o)}}function RM(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function NM(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Hn(n)&&(e[""]=t)}}function kM(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function qp(t,n,e,i,r,o,s,a){let c=n[N],l=c.consts,d=zn(l,s),f=qo(c,t,e,i,d);return o&&Bb(c,n,f,zn(l,a),r),f.mergedAttrs=Ho(f.mergedAttrs,f.attrs),f.attrs!==null&&sd(f,f.attrs,!1),f.mergedAttrs!==null&&sd(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function Yp(t,n){x_(t,n),sh(n)&&t.queries.elementEnd(n)}function OM(t,n,e,i,r,o){let s=n.consts,a=zn(s,r),c=qo(n,t,e,i,a);if(c.mergedAttrs=Ho(c.mergedAttrs,c.attrs),o!=null){let l=zn(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&sd(c,c.attrs,!1),c.mergedAttrs!==null&&sd(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var Hb=typeof ShadowRoot<"u",FM=typeof Document<"u";function PM(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&yd.SignalBased)!==0};return r&&(o.transform=r),o})}function LM(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function VM(t,n,e){let i=n instanceof be?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new id(e,i):e}function jM(t){let n=t.get(qe,null);if(n===null)throw new E(407,!1);let e=t.get(Ob,null),i=t.get(Bn,null),r=t.get(qn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function BM(t,n){let e=zb(t);return eb(n,e,e==="svg"?ah:e==="math"?ly:null)}function UM(t){if(t?.toLowerCase()==="script")throw new E(905,!1)}function zb(t){return(t.selectors[0][0]||"div").toLowerCase()}var $o=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=PM(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=LM(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=rS(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){ye(he.DynamicComponentStart);let a=B(null);try{let c=this.componentDef,l=VM(c,r||this.ngModule,n),d=jM(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate(Fb(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{B(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=HM(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?RS(l,r,a.encapsulation,e):BM(a,l);UM(d?.tagName);let f=e.get(zr,null),h=zM(d,()=>e.get(V,null)??z_());f&&f.addHost(h);let g=s?.some(u_)||o?.some(R=>typeof R!="function"&&R.bindings.some(u_)),b=Pp(null,c,null,512|gb(a),null,null,n,l,e,null,W_(d,e,!0));f&&Hb&&h instanceof ShadowRoot&&Al(b,()=>{f.removeHost(h)}),b[ze]=d,Ol(b);let w=null;try{let R=qp(ze,b,2,"#host",()=>c.directiveRegistry,!0,0);nb(l,d,R),zo(d,b),_d(c,b,R),_p(c,R,b),Yp(c,R),i!==void 0&&GM(R,this.ngContentSelectors,i),w=hn(R.index,b),b[ut]=w[ut],Hp(c,b,null)}catch(R){throw w!==null&&$h(w),$h(b),R}finally{ye(he.DynamicComponentEnd),Fl()}return new ad(this.componentType,b,!!g)}};function HM(t,n,e,i){let r=t?["ng-version","22.0.5"]:oS(n.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[op].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let h of f.bindings){a+=h[op].requiredVars;let g=d+1;h.create&&(h.targetIdx=g,(o??=[]).push(h)),h.update&&(h.targetIdx=g,(s??=[]).push(h))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,h=Kf(f);c.push(h)}return Fp(0,null,$M(o,s),1,a,c,null,null,null,[r],null)}function zM(t,n){let e=t.getRootNode?.();return FM&&e instanceof Document?e.head:e&&Hb&&e instanceof ShadowRoot?e:n().head}function $M(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function u_(t){let n=t[op].kind;return n==="input"||n==="twoWay"}var ad=class extends kb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Sl(e[N],ze),this.location=Wo(this._tNode,e),this.instance=hn(this._tNode.index,e)[ut],this.hostView=this.changeDetectorRef=new Gi(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Up(i,r[N],r,n,e);this.previousInputValues.set(n,e);let s=hn(i.index,r);$p(s,1)}get injector(){return new $i(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function GM(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Pt=(()=>{class t{static __NG_ELEMENT_ID__=WM}return t})();function WM(){let t=We();return $b(t,q())}var sp=class t extends Pt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Wo(this._hostTNode,this._hostLView)}get injector(){return new $i(this._hostTNode,this._hostLView)}get parentInjector(){let n=gp(this._hostTNode,this._hostLView);if(M_(n)){let e=Xl(n,this._hostLView),i=Ql(n),r=e[N].data[i+8];return new $i(r,e)}else return new $i(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=f_(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-ft}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=rp(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,td(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new $o(Oi(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let J=this.parentInjector.get(be,null);J&&(o=J)}let h=Oi(d.componentType??{}),g=rp(this._lContainer,h?.id??null),b=g?.firstChild??null,w=d.create(f,r,b,o,s,a);return this.insertImpl(w.hostView,c,td(this._hostTNode,g)),w}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(fy(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[ot],l=new t(c,c[_t],c[ot]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Gp(s,r,o,i),n.attachToViewContainerRef(),Jf(Oh(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=f_(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=nd(this._lContainer,e);i&&(Ys(Oh(this._lContainer),e),Np(i[N],i))}detach(n){let e=this._adjustIndex(n,-1),i=nd(this._lContainer,e);return i&&Ys(Oh(this._lContainer),e)!=null?new Gi(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function f_(t){return t[Qs]}function Oh(t){return t[Qs]||(t[Qs]=[])}function $b(t,n){let e,i=n[t.index];return dn(i)?e=i:(e=Rb(i,n,null,t),n[t.index]=e,Lp(n,e)),YM(e,n,t,i),new sp(e,t,n)}function qM(t,n){let e=t[De],i=e.createComment(""),r=fn(n,t),o=e.parentNode(r);return ed(e,o,i,e.nextSibling(r),!1),i}var YM=QM,ZM=()=>!1;function KM(t,n,e){return ZM(t,n,e)}function QM(t,n,e,i){if(t[Bi])return;let r;e.type&8?r=un(i):r=qM(n,e),t[Bi]=r}var ap=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},cp=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Kp(n,e).matches!==null&&this.queries[e].setDirty()}},cd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=nT(n):this.predicate=n}},lp=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},dp=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,XM(e,o)),this.matchTNodeWithReadOption(n,e,Yl(e,n,o,!1,!1))}else i===mn?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Yl(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===H||r===Pt||r===mn&&e.type&4)this.addMatch(e.index,-2);else{let o=Yl(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function XM(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function JM(t,n){return t.type&11?Wo(t,n):t.type&4?bd(t,n):null}function eT(t,n,e,i){return e===-1?JM(n,t):e===-2?tT(t,n,i):aa(t,t[N],e,n)}function tT(t,n,e){if(e===H)return Wo(n,t);if(e===mn)return bd(n,t);if(e===Pt)return $b(n,t)}function Gb(t,n,e,i){let r=n[Un].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(eT(n,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function up(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=Gb(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let f=ft;f<d.length;f++){let h=d[f];h[Vi]===h[ot]&&up(h[N],h,l,i)}if(d[Nr]!==null){let f=d[Nr];for(let h=0;h<f.length;h++){let g=f[h];up(g[N],g,l,i)}}}}}return i}function Zp(t,n){return t[Un].queries[n].queryList}function Wb(t,n,e){let i=new vi((e&4)===4);return my(t,n,i,i.destroy),(n[Un]??=new cp).queries.push(new ap(i))-1}function qb(t,n,e){let i=Re();return i.firstCreatePass&&(Zb(i,new cd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Wb(i,q(),n)}function Yb(t,n,e,i){let r=Re();if(r.firstCreatePass){let o=We();Zb(r,new cd(n,e,i),o.index),iT(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Wb(r,q(),e)}function nT(t){return t.split(",").map(n=>n.trim())}function Zb(t,n,e){t.queries===null&&(t.queries=new lp),t.queries.track(new dp(n,e))}function iT(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Kp(t,n){return t.queries.getByIndex(n)}function Kb(t,n){let e=t[N],i=Kp(e,n);return i.crossesNgTemplate?up(e,t,n,[]):Gb(e,t,i,n)}function Qb(t,n,e){let i,r=Rs(()=>{i._dirtyCounter();let o=rT(i,t);if(n&&o===void 0)throw new E(-951,!1);return o});return i=r[Be],i._dirtyCounter=ie(0),i._flatValue=void 0,r}function Qp(t){return Qb(!0,!1,t)}function Xp(t){return Qb(!0,!0,t)}function Xb(t,n){let e=t[Be];e._lView=q(),e._queryIndex=n,e._queryList=Zp(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function rT(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[U]&4)return n?void 0:yt;let r=Zp(e,i),o=Kb(e,i);return r.reset(o,L_),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function qi(t){return!!t&&typeof t.then=="function"}function Jp(t){return!!t&&typeof t.subscribe=="function"}var _i=class{},Cd=class{};var ld=class extends _i{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=qv(n);this._bootstrapComponents=qI(o.bootstrap),this._r3Injector=xh(n,e,[{provide:_i,useValue:this},...i],Ws(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},dd=class extends Cd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new ld(this.moduleType,n,[])}};var da=class extends _i{injector;instance=null;constructor(n){super();let e=new Sr([...n.providers,{provide:_i,useValue:this}],n.parent||Ao(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function va(t,n,e=null){return new da({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var oT=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=th(!1,e.type),r=i.length>0?va([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=F({token:t,providedIn:"environment",factory:()=>new t(M(be))})}return t})();function O(t){return fa(()=>{let n=Jb(t),e=j(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==vp.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(oT).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Nn.Emulated,styles:t.styles||yt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Wi("NgStandalone"),eC(e);let i=t.dependencies;return e.directiveDefs=h_(i,sT),e.pipeDefs=h_(i,Yv),e.id=lT(e),e})}function sT(t){return Oi(t)||Kf(t)}function G(t){return fa(()=>({type:t.type,bootstrap:t.bootstrap||yt,declarations:t.declarations||yt,imports:t.imports||yt,exports:t.exports||yt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function aT(t,n){if(t==null)return Pi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=yd.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function cT(t){if(t==null)return Pi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function P(t){return fa(()=>{let n=Jb(t);return eC(n),n})}function Jb(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Pi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||yt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:aT(t.inputs,n),outputs:cT(t.outputs),debugInfo:null}}function eC(t){t.features?.forEach(n=>n(t))}function h_(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function lT(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var tC=new _("");var em=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(tC,{optional:!0})??[];injector=u(ce);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=st(this.injector,r);if(qi(o))e.push(o);else if(Jp(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function tm(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function dT(t){return Object.getPrototypeOf(t.prototype).constructor}function Ze(t){let n=dT(t.type),e=!0,i=[t];for(;n;){let r;if(Hn(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new E(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=Fh(t.inputs),s.declaredInputs=Fh(t.declaredInputs),s.outputs=Fh(t.outputs);let a=r.hostBindings;a&&mT(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&hT(t,c),l&&pT(t,l),uT(t,r),Wv(t.outputs,r.outputs),Hn(r)&&r.data.animation){let d=t.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===Ze&&(e=!1)}}n=Object.getPrototypeOf(n)}fT(i)}function uT(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function fT(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Ho(r.hostAttrs,e=Ho(e,r.hostAttrs))}}function Fh(t){return t===Pi?{}:t===yt?[]:t}function hT(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function pT(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function mT(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function nC(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Ho(t.mergedAttrs,t.attrs);let d=t.tView=Fp(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Po(t,!1);let c=vT(e,n,t,i);Ll()&&kp(e,n,c,t),zo(c,n);let l=Rb(c,n,c,t);n[i+ze]=l,Lp(n,l),KM(l,t,n)}function gT(t,n,e,i,r,o,s,a,c,l,d){let f=e+ze,h;return n.firstCreatePass?(h=qo(n,f,4,s||null,a||null),Rl()&&Bb(n,t,h,zn(n.consts,l),Vp),x_(n,h)):h=n.data[f],nC(h,t,n,e,i,r,o,c),Oo(h)&&_d(n,t,h),l!=null&&ga(t,h,d),h}function nm(t,n,e,i,r,o,s,a,c,l,d){let f=e+ze,h;if(n.firstCreatePass){if(h=qo(n,f,4,s||null,a||null),l!=null){let g=zn(n.consts,l);h.localNames=[];for(let b=0;b<g.length;b+=2)h.localNames.push(g[b],-1)}}else h=n.data[f];return nC(h,t,n,e,i,r,o,c),l!=null&&ga(t,h,d),h}function re(t,n,e,i,r,o,s,a){let c=q(),l=Re(),d=zn(l.consts,o);return gT(c,l,t,n,e,i,r,d,void 0,s,a),re}var vT=yT;function yT(t,n,e,i){return ea(!0),n[De].createComment("")}var Ed=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var im=new _("");var ya=new _("");function iC(){uf(()=>{let t="";throw new E(600,t)})}var _T=10;var Jt=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(Xt);afterRenderManager=u(md);zonelessEnabled=u(na);rootEffectScheduler=u(Bl);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new I;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(gi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Q(e=>!e))}constructor(){u(qn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(be);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=ce.NULL){return this._injector.get(k).run(()=>{if(ye(he.BootstrapComponentStart),!this._injector.get(em).done){let J="";throw new E(405,J)}let a=Oi(e),c=this._injector.get(_i),l=new $o(a,c);this.componentTypes.push(e);let{hostElement:d,directives:f,bindings:h}=bT(i),g=d||l.selector,b=l.create(r,[],g,c.injector,f,h),w=b.location.nativeElement,R=b.injector.get(im,null);return R?.registerApplication(w),b.onDestroy(()=>{this.detachView(b.hostView),sa(this.components,b),R?.unregisterApplication(w)}),this._loadComponent(b),ye(he.BootstrapComponentEnd,b),b})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ye(he.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(pd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ye(he.ChangeDetectionEnd),new E(101,!1);let e=B(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,B(e),this.afterTick.next(),ye(he.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(qe,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<_T;){ye(he.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ye(he.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Xs(r))continue;let o=i&&!this.zonelessEnabled?0:1;Sb(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Xs(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;sa(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(ya,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>sa(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new E(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function bT(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function sa(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function pe(t,n,e,i){let r=q(),o=Fr();if(Ei(r,o,n)){let s=Re(),a=Lo();jS(a,r,t,n,e,i)}return pe}function Ce(t,n,e,i,r,o,s,a){Wi("NgControlFlow");let c=q(),l=Re(),d=zn(l.consts,o);return nm(c,l,t,n,e,i,r,d,256,s,a),rm}function rm(t,n,e,i,r,o,s,a){Wi("NgControlFlow");let c=q(),l=Re(),d=zn(l.consts,o);return nm(c,l,t,n,e,i,r,d,512,s,a),rm}function Ee(t,n){Wi("NgControlFlow");let e=q(),i=Fr(),r=e[i]!==gn?e[i]:-1,o=r!==-1?p_(e,ze+r):void 0,s=0;if(Ei(e,i,t)){let a=B(null);try{if(o!==void 0&&sM(o,s),t!==-1){let c=ze+t,l=p_(e,c),d=CT(e[N],c),f=pM(l,d,e),h=zp(e,d,n,{dehydratedView:f});Gp(l,h,s,td(d,f))}}finally{B(a)}}else if(o!==void 0){let a=oM(o,s);a!==void 0&&(a[ut]=n)}}function p_(t,n){return t[n]}function CT(t,n){return Sl(t,n)}function x(t,n,e){let i=q(),r=Fr();if(Ei(i,r,n)){let o=Re(),s=Lo();bb(s,i,t,n,i[De],e)}return x}function fp(t,n,e,i,r){Up(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=q(),o=r[N],s=t+ze,a=o.firstCreatePass?qp(s,r,2,n,Vp,Rl(),e,i):o.data[s];if(mi(a)){let c=r[Mn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(Fb(l),()=>(m_(t,n,r,a,i),m))}}return m_(t,n,r,a,i),m}function m_(t,n,e,i,r){if(jp(i,e,t,n,rC),Oo(i)){let o=e[N];_d(o,e,i),_p(o,i,e)}r!=null&&ga(e,i)}function p(){let t=Re(),n=We(),e=Bp(n);return t.firstCreatePass&&Yp(t,e),mh(e)&&gh(),hh(),e.classesWithoutHost!=null&&Zx(e)&&fp(t,e,q(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Kx(e)&&fp(t,e,q(),e.stylesWithoutHost,!1),p}function ue(t,n,e,i){return m(t,n,e,i),p(),ue}function Tt(t,n,e,i){let r=q(),o=r[N],s=t+ze,a=o.firstCreatePass?OM(s,o,2,n,e,i):o.data[s];return jp(a,r,t,n,rC),i!=null&&ga(r,a),Tt}function At(){let t=We(),n=Bp(t);return mh(n)&&gh(),hh(),At}function vn(t,n,e,i){return Tt(t,n,e,i),At(),vn}var rC=(t,n,e,i,r)=>(ea(!0),eb(n[De],i,Dh()));function om(t,n,e){let i=q(),r=i[N],o=t+ze,s=r.firstCreatePass?qp(o,i,8,"ng-container",Vp,Rl(),n,e):r.data[o];if(jp(s,i,t,"ng-container",ET),Oo(s)){let a=i[N];_d(a,i,s),_p(a,s,i)}return e!=null&&ga(i,s),om}function sm(){let t=Re(),n=We(),e=Bp(n);return t.firstCreatePass&&Yp(t,e),sm}function Yo(t,n,e){return om(t,n,e),sm(),Yo}var ET=(t,n,e,i,r)=>(ea(!0),UI(n[De],""));function at(){return q()}function Zn(t,n,e){let i=q(),r=Fr();if(Ei(i,r,n)){let o=Re(),s=Lo();Cb(s,i,t,n,i[De],e)}return Zn}var _a="en-US";var wT=_a;function oC(t){typeof t=="string"&&(wT=t.toLowerCase().replace(/_/g,"-"))}function $(t,n,e){let i=q(),r=Re(),o=We();return sC(r,i,i[De],o,t,n,e),$}function sC(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Uo(i,n,o),Vb(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let h=d[f],g=d[f+1];c??=Uo(i,n,o),rd(i,n,h,g,r,c)}if(l&&l.length)for(let f of l)c??=Uo(i,n,o),rd(i,n,f,r,r,c)}}function S(t=1){return Ty(t)}function DT(t,n){let e=null,i=JI(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?ab(t,o,!0):nS(i,o))return r}return e}function Ke(t){let n=q()[Ft][_t];if(!n.projection){let e=t?t.length:1,i=n.projection=ey(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?DT(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function le(t,n=0,e,i,r,o){let s=q(),a=Re(),c=i?t+1:null;c!==null&&nm(s,a,c,i,r,o,null,e);let l=qo(a,ze+t,16,null,e||null);l.projection===null&&(l.projection=n),_h();let f=!s[Ro]||ph();s[Ft][_t].projection[l.projection]===null&&c!==null?xT(s,a,c):f&&!fd(l)&&xS(a,s,l)}function xT(t,n,e){let i=ze+e,r=n.data[i],o=t[i],s=rp(o,r.tView.ssrId),a=zp(t,r,void 0,{dehydratedView:s});Gp(o,a,0,td(r,s))}function Kn(t,n,e,i){return Yb(t,n,e,i),Kn}function Lt(t,n,e){return qb(t,n,e),Lt}function oe(t){let n=q(),e=Re(),i=kl();Js(i+1);let r=Kp(e,i);if(t.dirty&&uy(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Kb(n,i);t.reset(o,L_),t.notifyOnChanges()}return!0}return!1}function se(){return Zp(q(),kl())}function wd(t,n,e,i,r){return Xb(n,Yb(t,e,i,r)),wd}function Dd(t,n,e,i){return Xb(t,qb(n,e,i)),Dd}function xd(t=1){Js(kl()+t)}function Yi(t){let n=_y();return dy(n,ze+t)}function Gl(t,n){return t<<17|n<<2}function Hr(t){return t>>17&32767}function IT(t){return(t&2)==2}function ST(t,n){return t&131071|n<<17}function hp(t){return t|2}function Go(t){return(t&131068)>>2}function Ph(t,n){return t&-131069|n<<2}function MT(t){return(t&1)===1}function pp(t){return t|1}function TT(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Hr(s),c=Go(s);t[i]=e;let l=!1,d;if(Array.isArray(e)){let f=e;d=f[1],(d===null||To(f,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let h=Hr(t[a+1]);t[i+1]=Gl(h,a),h!==0&&(t[h+1]=Ph(t[h+1],i)),t[a+1]=ST(t[a+1],i)}else t[i+1]=Gl(a,0),a!==0&&(t[a+1]=Ph(t[a+1],i)),a=i;else t[i+1]=Gl(c,0),a===0?a=i:t[c+1]=Ph(t[c+1],i),c=i;l&&(t[i+1]=hp(t[i+1])),g_(t,d,i,!0),g_(t,d,i,!1),AT(n,d,t,i,o),s=Gl(a,c),o?n.classBindings=s:n.styleBindings=s}function AT(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&To(o,n)>=0&&(e[i+1]=pp(e[i+1]))}function g_(t,n,e,i){let r=t[e+1],o=n===null,s=i?Hr(r):Go(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];RT(c,n)&&(a=!0,t[s+1]=i?pp(l):hp(l)),s=i?Hr(l):Go(l)}a&&(t[e+1]=i?hp(r):pp(r))}function RT(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?To(t,n)>=0:!1}var Rn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function NT(t){return t.substring(Rn.key,Rn.keyEnd)}function kT(t){return OT(t),aC(t,cC(t,0,Rn.textEnd))}function aC(t,n){let e=Rn.textEnd;return e===n?-1:(n=Rn.keyEnd=FT(t,Rn.key=n,e),cC(t,n,e))}function OT(t){Rn.key=0,Rn.keyEnd=0,Rn.value=0,Rn.valueEnd=0,Rn.textEnd=t.length}function cC(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function FT(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function $r(t,n,e){return lC(t,n,e,!1),$r}function ae(t,n){return lC(t,n,null,!0),ae}function On(t){LT(zT,PT,t,!0)}function PT(t,n){for(let e=kT(n);e>=0;e=aC(n,e))Dl(t,NT(n),!0)}function lC(t,n,e,i){let r=q(),o=Re(),s=Ch(2);if(o.firstUpdatePass&&uC(o,t,s,i),n!==gn&&Ei(r,s,n)){let a=o.data[Ui()];fC(o,a,r,r[De],t,r[s+1]=GT(n,e),i,s)}}function LT(t,n,e,i){let r=Re(),o=Ch(2);r.firstUpdatePass&&uC(r,null,o,i);let s=q();if(e!==gn&&Ei(s,o,e)){let a=r.data[Ui()];if(hC(a,i)&&!dC(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=yl(c,e||"")),fp(r,a,s,e,i)}else $T(r,a,s,s[De],s[o+1],s[o+1]=HT(t,n,e),i,o)}}function dC(t,n){return n>=t.expandoStartIndex}function uC(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Ui()],s=dC(t,e);hC(o,i)&&n===null&&!s&&(n=!1),n=VT(r,o,n,i),TT(r,o,n,e,s,i)}}function VT(t,n,e,i){let r=xy(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Lh(null,t,n,e,i),e=ua(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=Lh(r,t,n,e,i),o===null){let c=jT(t,n,i);c!==void 0&&Array.isArray(c)&&(c=Lh(null,t,n,c[1],i),c=ua(c,n.attrs,i),BT(t,n,i,c))}else o=UT(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function jT(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Go(i)!==0)return t[Hr(i)]}function BT(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Hr(r)]=i}function UT(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=ua(i,s,e)}return ua(i,n.attrs,e)}function Lh(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=ua(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function ua(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Dl(t,s,e?!0:n[++o]))}return t===void 0?null:t}function HT(t,n,e){if(e==null||e==="")return yt;let i=[],r=kn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function zT(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Dl(t,i,e)}function $T(t,n,e,i,r,o,s,a){r===gn&&(r=yt);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let h=c<r.length?r[c+1]:void 0,g=l<o.length?o[l+1]:void 0,b=null,w;d===f?(c+=2,l+=2,h!==g&&(b=f,w=g)):f===null||d!==null&&d<f?(c+=2,b=d):(l+=2,b=f,w=g),b!==null&&fC(t,n,e,i,b,w,s,a),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function fC(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],d=MT(l)?v_(c,n,e,r,Go(l),s):void 0;if(!ud(d)){ud(o)||IT(l)&&(o=v_(c,null,e,r,a,s));let f=ch(Ui(),e);SS(i,s,f,r,o)}}function v_(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,h=e[r+1];h===gn&&(h=f?yt:void 0);let g=f?xl(h,i):d===i?h:void 0;if(l&&!ud(g)&&(g=xl(c,i)),ud(g)&&(a=g,s))return a;let b=t[r+1];r=s?Hr(b):Go(b)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=xl(c,i))}return a}function ud(t){return t!==void 0}function GT(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Ws(kn(t)))),t}function hC(t,n){return(t.flags&(n?8:16))!==0}function y(t,n=""){let e=q(),i=Re(),r=t+ze,o=i.firstCreatePass?qo(i,r,1,n,null):i.data[r],s=WT(i,e,o,n);e[r]=s,Ll()&&kp(i,e,s,o),Po(o,!1)}var WT=(t,n,e,i)=>(ea(!0),jI(n[De],i));function qT(t,n,e,i=""){return Ei(t,Fr(),e)?n+Cl(e)+i:gn}function it(t){return Te("",t),it}function Te(t,n,e){let i=q(),r=qT(i,t,n,e);return r!==gn&&YT(i,Ui(),r),Te}function YT(t,n,e){let i=ch(n,t);BI(t[De],i,e)}function ke(t,n,e){Ul(n)&&(n=n());let i=q(),r=Fr();if(Ei(i,r,n)){let o=Re(),s=Lo();bb(s,i,t,n,i[De],e)}return ke}function Ve(t,n){let e=Ul(t);return e&&t.set(n),e}function Oe(t,n){let e=q(),i=Re(),r=We();return sC(i,e,e[De],r,t,n),Oe}function y_(t,n,e){let i=Re();i.firstCreatePass&&pC(n,i.data,i.blueprint,Hn(t),e)}function pC(t,n,e,i,r){if(t=dt(t),Array.isArray(t))for(let o=0;o<t.length;o++)pC(t[o],n,e,i,r);else{let o=Re(),s=q(),a=We(),c=Ir(t)?t:dt(t.provide),l=ih(t),d=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(Ir(t)||!t.multi){let g=new jr(l,r,D,null),b=jh(c,n,r?d:d+h,f);b===-1?(Hh(Jl(a,s),o,c),Vh(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(g),s.push(g)):(e[b]=g,s[b]=g)}else{let g=jh(c,n,d+h,f),b=jh(c,n,d,d+h),w=g>=0&&e[g],R=b>=0&&e[b];if(r&&!R||!r&&!w){Hh(Jl(a,s),o,c);let J=QT(r?KT:ZT,e.length,r,i,l,t);!r&&R&&(e[b].providerFactory=J),Vh(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(J),s.push(J)}else{let J=mC(e[r?b:g],l,!r&&i);Vh(o,t,g>-1?g:b,J)}!r&&i&&R&&e[b].componentProviders++}}}function Vh(t,n,e,i){let r=Ir(n),o=sy(n);if(r||o){let c=(o?dt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function mC(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function jh(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function ZT(t,n,e,i,r){return mp(this.multi,[])}function KT(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=aa(i,i[N],this.providerFactory.index,r);s=c.slice(0,a),mp(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],mp(o,s);return s}function mp(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function QT(t,n,e,i,r,o){let s=new jr(t,e,D,null);return s.multi=[],s.index=n,s.componentProviders=0,mC(s,r,i&&!e),s}function Qe(t,n){return e=>{e.providersResolver=(i,r)=>y_(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>y_(i,r?r(n):n,!0))}}function ba(t,n,e){return JT(q(),by(),t,n,e)}function XT(t,n){let e=t[n];return e===gn?void 0:e}function JT(t,n,e,i,r,o){let s=n+e;return Ei(t,s,r)?gM(t,s+1,o?i.call(o,r):i(r)):XT(t,s+1)}function Ca(t,n){return bd(t,n)}var gC=(()=>{class t{applicationErrorHandler=u(Xt);appRef=u(Jt);taskService=u(gi);ngZone=u(k);zonelessEnabled=u(na);tracing=u(qn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new de;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get($s):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(Ah,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ky:Ih;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get($s+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function vC(){return[{provide:Bn,useExisting:gC},{provide:k,useClass:Gs},{provide:na,useValue:!0}]}var am=(()=>{class t{compileModuleSync(e){return new dd(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function eA(){return typeof $localize<"u"&&$localize.locale||_a}var Id=new _("",{factory:()=>u(Id,{optional:!0,skipSelf:!0})||eA()});function Rt(t,n){return Rs(t,n?.equal)}function fe(t){return hv(t)}var tA=t=>t;function cm(t,n){if(typeof t=="function"){let e=gf(t,tA,n?.equal);return yC(e,n?.debugName)}else{let e=gf(t.source,t.computation,t.equal);return yC(e,t.debugName)}}function yC(t,n){let e=t[Be],i=t;return i.set=r=>uv(e,r),i.update=r=>fv(e,r),i.asReadonly=Vl.bind(t),i}var SC=Symbol("InputSignalNode#UNSET"),_A=j(C({},Ns),{transformFn:void 0,applyValueToInputSignal(t,n){pr(t,n)}});function MC(t,n){let e=Object.create(_A);e.value=t,e.transformFn=n?.transform;function i(){if(Ai(e),e.value===SC){let r=null;throw new E(-950,r)}return e.value}return i[Be]=e,i}var Zi=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>ha(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},TC=(()=>{let t=new _("");return t.__NG_ELEMENT_ID__=n=>{let e=We();if(e===null)throw new E(-204,!1);if(e.type&2)return e.value;if(n&8)return null;throw new E(-204,!1)},t})();function gm(t){return bA(t)?t.default:t}function bA(t){return t&&typeof t=="object"&&"default"in t}function _C(t,n){return MC(t,n)}function CA(t){return MC(SC,t)}var Zo=(_C.required=CA,_C);function bC(t,n){return Qp(n)}function EA(t,n){return Xp(n)}var wa=(bC.required=EA,bC);function CC(t,n){return Qp(n)}function wA(t,n){return Xp(n)}var AC=(CC.required=wA,CC);var DA=1e4;var VW=DA-1e3;var dm=class{supports(n){return Wp(n)}create(n){return new um(n)}},xA=(t,n)=>n,um=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||xA}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<EC(i,r,o)?e:i,a=EC(s,r,o),c=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let l=a-r,d=c-r;if(l!=d){for(let h=0;h<l;h++){let g=h<o.length?o[h]:o[h]=0,b=g+h;d<=b&&b<l&&(o[h]=g+1)}let f=s.previousIndex;o[f]=d-l}}a!==c&&n(s,a,c)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!Wp(n))throw new E(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,Pb(n,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new fm(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Md),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Md),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},fm=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},hm=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Md=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new hm,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function EC(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function wC(){return new Td([new dm])}var Td=(()=>{class t{factories;static \u0275prov=F({token:t,providedIn:"root",factory:wC});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=u(t,{optional:!0,skipSelf:!0});return t.create(e,i||wC())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new E(901,!1)}}return t})();var me=(()=>{class t{static __NG_ELEMENT_ID__=IA}return t})();function IA(t){return SA(We(),q(),(t&16)===16)}function SA(t,n,e){if(mi(t)&&!e){let i=hn(t.index,n);return new Gi(i,i)}else if(t.type&175){let i=n[Ft];return new Gi(i,n)}return null}var pm=new _(""),MA=new _("");function Ea(t){return!t.moduleRef}function TA(t){let n=Ea(t)?t.r3Injector:t.moduleRef.injector,e=n.get(k);return e.run(()=>{Ea(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Xt),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ea(t)){let o=()=>n.destroy(),s=t.platformInjector.get(pm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(pm);s.add(o),t.moduleRef.onDestroy(()=>{sa(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return RA(i,e,()=>{let o=n.get(gi),s=o.add(),a=n.get(em);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(Id,_a);if(oC(c||_a),!n.get(MA,!0))return Ea(t)?n.get(Jt):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ea(t)){let d=n.get(Jt);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return AA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var AA;function RA(t,n,e){try{let i=e();return qi(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Sd=null;function NA(t=[],n){return ce.create({name:n,providers:[{provide:Ks,useValue:"platform"},{provide:pm,useValue:new Set([()=>Sd=null])},...t]})}function kA(t=[]){if(Sd)return Sd;let n=NA(t);return Sd=n,iC(),OA(n),n}function OA(t){let n=t.get(jl,null);st(t,()=>{n?.forEach(e=>e())})}function RC(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;ye(he.BootstrapApplicationStart);try{let o=r?.injector??kA(i),s=[vC(),Fy,...e||[]],a=new da({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return TA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{ye(he.BootstrapApplicationEnd)}}function W(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function yn(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var lm=Symbol("NOT_SET"),NC=new Set,FA=j(C({},Ns),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:lm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==lm&&!vo(this))return this.signal;try{for(let r of this.cleanup??NC)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=oi(this),i;try{i=this.userFn.apply(null,n)}finally{Ri(this,e)}return(this.value===lm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),mm=class extends ca{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(ht),s),this.scheduler=r;for(let a of Tp){let c=e[a];if(c===void 0)continue;let l=Object.create(FA);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Ai(l),l.value),l.signal[Be]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??NC)e()}finally{Ni(n)}}};function kC(t,n){let e=n?.injector??u(ce),i=e.get(Bn),r=e.get(md),o=e.get(qn,null,{optional:!0});r.impl??=e.get(Ap);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Vo,null,{optional:!0}),c=new mm(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Ad(t,n){let e=Oi(t),i=n.elementInjector||Ao();return new $o(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var OC=null;function _n(){return OC}function vm(t){OC??=t}var Da=class{},Ko=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:()=>u(FC),providedIn:"platform"})}return t})();var FC=(()=>{class t extends Ko{_location;_history;_doc=u(V);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return _n().getBaseHref(this._doc)}onPopState(e){let i=_n().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=_n().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function VC(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function PC(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Qi(t){return t&&t[0]!=="?"?`?${t}`:t}var Qo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:()=>u(LA),providedIn:"root"})}return t})(),PA=new _(""),LA=(()=>{class t extends Qo{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(V).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return VC(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Qi(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+Qi(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+Qi(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(M(Ko),M(PA,8))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Xi=(()=>{class t{_subject=new I;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=BA(PC(LC(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Qi(i))}normalize(e){return t.stripTrailingSlash(jA(this._basePath,LC(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Qi(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Qi(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Qi;static joinWithSlash=VC;static stripTrailingSlash=PC;static \u0275fac=function(i){return new(i||t)(M(Qo))};static \u0275prov=F({token:t,factory:()=>VA(),providedIn:"root"})}return t})();function VA(){return new Xi(M(Qo))}function jA(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function LC(t){return t.replace(/\/index\.html$/,"")}function BA(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Rd=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},bn=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Rd(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),jC(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);jC(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(D(Pt),D(mn),D(Td))};static \u0275dir=P({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function jC(t,n){t.context.$implicit=n.item}var pt=(()=>{class t{_viewContainer;_context=new Nd;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){BC(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){BC(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(D(Pt),D(mn))};static \u0275dir=P({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),Nd=class{$implicit=null;ngIf=null};function BC(t,n){if(t&&!t.createEmbeddedView)throw new E(2020,!1)}var xa=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(ce);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(D(Pt))};static \u0275dir=P({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ye]})}return t})();var ct=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({})}return t})();function Ia(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var ym="browser";function UC(t){return t===ym}var Sa=class{_doc;constructor(n){this._doc=n}manager},kd=(()=>{class t extends Sa{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(M(V))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),Pd=new _(""),Em=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof kd));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof kd);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new E(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(M(Pd),M(k))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),_m="ng-app-id";function HC(t){for(let n of t)n.remove()}function zC(t,n){let e=n.createElement("style");return e.textContent=t,e}function HA(t,n,e,i){let r=t.head?.querySelectorAll(`style[${_m}="${n}"],link[${_m}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(_m),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Cm(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var wm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,HA(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,zC);i?.forEach(r=>this.addUsage(r,this.external,Cm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(HC(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])HC(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,zC(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Cm(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(M(V),M(Pr),M(Vr,8),M(Lr))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),bm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Dm=/%COMP%/g;var GC="%COMP%",zA=`_nghost-${GC}`,$A=`_ngcontent-${GC}`,GA=!0,WA=new _("",{factory:()=>GA});function qA(t){return $A.replace(Dm,t)}function YA(t){return zA.replace(Dm,t)}function WC(t,n){return n.map(e=>e.replace(Dm,t))}var xm=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Ma(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Fd?r.applyToHost(e):r instanceof Ta&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Nn.Emulated:o=new Fd(c,l,i,this.appId,d,s,a,f);break;case Nn.ShadowDom:return new Od(c,e,i,s,a,this.nonce,f,l);case Nn.ExperimentalIsolatedShadowDom:return new Od(c,e,i,s,a,this.nonce,f);default:o=new Ta(c,l,i,d,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(M(Em),M(zr),M(Pr),M(WA),M(V),M(k),M(Vr),M(qn,8))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),Ma=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(bm[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){($C(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&($C(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new E(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=bm[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=bm[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Gn.DashCase|Gn.Important)?n.style.setProperty(e,i,r&Gn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Gn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=_n().getGlobalEventTarget(this.doc,n),!n))throw new E(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function $C(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Od=class extends Ma{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=WC(i.id,l);for(let f of l){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=f,this.shadowRoot.appendChild(h)}let d=i.getExternalStyles?.();if(d)for(let f of d){let h=Cm(f,r);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ta=class extends Ma{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?WC(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Br.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Fd=class extends Ta{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=qA(l),this.hostAttr=YA(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Ld=class t extends Da{supportsDOMEvents=!0;static makeCurrent(){vm(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=ZA();return e==null?null:KA(e)}resetBaseElement(){Aa=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Ia(document.cookie,n)}},Aa=null;function ZA(){return Aa=Aa||document.head.querySelector("base"),Aa?Aa.getAttribute("href"):null}function KA(t){return new URL(t,document.baseURI).pathname}var qC=["alt","control","meta","shift"],QA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},XA={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},YC=(()=>{class t extends Sa{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>_n().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),qC.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=QA[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),qC.forEach(s=>{if(s!==r){let a=XA[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(M(V))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})();function Im(t,n,e){return _e(this,null,function*(){let i=C({rootComponent:t},JA(n,e));return RC(i)})}function JA(t,n){return{platformRef:n?.platformRef,appProviders:[...rR,...t?.providers??[]],platformProviders:iR}}function eR(){Ld.makeCurrent()}function tR(){return new Mt}function nR(){return yp(document),document}var iR=[{provide:Lr,useValue:ym},{provide:jl,useValue:eR,multi:!0},{provide:V,useFactory:nR}];var rR=[{provide:Ks,useValue:"root"},{provide:Mt,useFactory:tR},{provide:Pd,useClass:kd,multi:!0},{provide:Pd,useClass:YC,multi:!0},xm,{provide:zr,useClass:wm},{provide:wm,useExisting:zr},Em,{provide:qe,useExisting:xm},[]];var xi=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var jd=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Bd=class{encodeKey(n){return ZC(n)}encodeValue(n){return ZC(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function oR(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var sR=/%(\d[a-f0-9])/gi,aR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function ZC(t){return encodeURIComponent(t).replace(sR,(n,e)=>aR[e]??n)}function Vd(t){return`${t}`}var Di=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Bd,n.fromString){if(n.fromObject)throw new E(2805,!1);this.map=oR(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Vd):[Vd(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Vd(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Vd(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function cR(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function KC(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function QC(t){return typeof Blob<"u"&&t instanceof Blob}function XC(t){return typeof FormData<"u"&&t instanceof FormData}function lR(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Sm="Content-Type",JC="Accept",t0="text/plain",n0="application/json",dR=`${n0}, ${t0}, */*`,Xo=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(cR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new E(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new xi,this.context??=new jd,!this.params)this.params=new Di,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let d=a.indexOf("?"),f=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+f+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||KC(this.body)||QC(this.body)||XC(this.body)||lR(this.body)?this.body:this.body instanceof Di?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||XC(this.body)?null:QC(this.body)?this.body.type||null:KC(this.body)?null:typeof this.body=="string"?t0:this.body instanceof Di?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?n0:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,h=n.integrity||this.integrity,g=n.referrerPolicy||this.referrerPolicy,b=n.transferCache??this.transferCache,w=n.timeout??this.timeout,R=n.body!==void 0?n.body:this.body,J=n.withCredentials??this.withCredentials,Je=n.reportProgress??this.reportProgress,vt=n.reportUploadProgress??this.reportUploadProgress,po=n.reportDownloadProgress??this.reportDownloadProgress,Ss=n.headers||this.headers,cr=n.params||this.params,Mc=n.context??this.context;return n.setHeaders!==void 0&&(Ss=Object.keys(n.setHeaders).reduce((mo,lr)=>mo.set(lr,n.setHeaders[lr]),Ss)),n.setParams&&(cr=Object.keys(n.setParams).reduce((mo,lr)=>mo.set(lr,n.setParams[lr]),cr)),new t(e,i,R,{params:cr,headers:Ss,context:Mc,reportProgress:Je,reportUploadProgress:vt,reportDownloadProgress:po,responseType:r,withCredentials:J,transferCache:b,keepalive:o,cache:a,priority:s,timeout:w,mode:c,redirect:l,credentials:d,referrer:f,integrity:h,referrerPolicy:g})}},Jo=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Jo||{}),es=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new xi,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Ud=class t extends es{constructor(n={}){super(n)}type=Jo.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Ra=class t extends es{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Jo.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Gr=class extends es{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},uR=200;var fR=/^\)\]\}',?\n/,eq=1024*1024,i0=new _("",{factory:()=>null}),Hd=(()=>{class t{fetchImpl=u(Tm,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=u(k);destroyRef=u(ht);maxResponseSize=u(i0);handle(e){return new ee(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(Am,s=>i.error(new Gr({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}doRequest(e,i,r){return _e(this,null,function*(){let o=this.createRequestInit(e),s;try{let R=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,C({signal:i},o)));hR(R),r.next({type:Jo.Sent}),s=yield R}catch(R){r.error(new Gr({error:R,status:R.status??0,statusText:R.statusText,url:e.urlWithParams,headers:R.headers}));return}let a=new xi(s.headers),c=s.statusText,l=s.url||e.urlWithParams,d=s.status,f=null,h=e.reportProgress||e.reportDownloadProgress;if(h&&r.next(new Ud({headers:a,status:d,statusText:c,url:l})),s.body){let R=s.headers.get("content-length"),J=R!==null?Number(R):NaN;this.maxResponseSize!==null&&Number.isFinite(J)&&J>this.maxResponseSize&&e0(this.maxResponseSize);let Je=[],vt=s.body.getReader(),po=0,Ss,cr,Mc=typeof Zone<"u"&&Zone.current,mo=!1;if(yield this.ngZone.runOutsideAngular(()=>_e(this,null,function*(){for(;;){if(this.destroyRef.destroyed){yield vt.cancel(),mo=!0;break}let{done:Ms,value:af}=yield vt.read();if(Ms)break;if(Je.push(af),po+=af.length,this.maxResponseSize!==null&&po>this.maxResponseSize&&(yield vt.cancel(),e0(this.maxResponseSize)),h){cr=e.responseType==="text"?(cr??"")+(Ss??=new TextDecoder).decode(af,{stream:!0}):void 0;let ev=()=>r.next({type:Jo.DownloadProgress,total:Number.isFinite(J)?J:void 0,loaded:po,partialText:cr});Mc?Mc.run(ev):ev()}}})),mo){r.complete();return}let lr=this.concatChunks(Je,po);try{let Ms=s.headers.get(Sm)??"";f=this.parseBody(e,lr,Ms,d)}catch(Ms){r.error(new Gr({error:Ms,headers:new xi(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=f?uR:0);let g=d>=200&&d<300,b=s.redirected,w=s.type;g?(r.next(new Ra({body:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:w})),r.complete()):r.error(new Gr({error:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:w}))})}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(fR,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new E(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(JC)||(i[JC]=dR),!e.headers.has(Sm)){let o=e.detectContentTypeHeader();o!==null&&(i[Sm]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Tm=class{};function Am(){}function hR(t){t.then(Am,Am)}function e0(t){throw new E(-2825,!1)}function pR(t,n){return n(t)}function mR(t,n,e){return(i,r)=>st(e,()=>n(i,o=>t(o,r)))}var Rm=new _("",{factory:()=>[]}),r0=new _(""),o0=new _("",{factory:()=>!0});var Nm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(Hd),r},providedIn:"root"})}return t})();var zd=(()=>{class t{backend;injector;chain=null;pendingTasks=u(ia);contributeToStability=u(o0);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Rm),...this.injector.get(r0,[])]));this.chain=i.reduceRight((r,o)=>mR(r,o,this.injector),pR)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Er(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(M(Nm),M(be))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),km=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(zd),r},providedIn:"root"})}return t})();function Mm(t,n){return C({body:n},t)}var Cn=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Xo)o=e;else{let c;r.headers instanceof xi?c=r.headers:c=new xi(r.headers);let l;r.params&&(r.params instanceof Di?l=r.params:l=new Di({fromObject:r.params})),o=new Xo(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=L(o).pipe(Io(c=>this.handler.handle(c)));if(e instanceof Xo||r.observe==="events")return s;let a=s.pipe(Me(c=>c instanceof Ra));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(Q(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new E(2806,!1);return c.body}));case"blob":return a.pipe(Q(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new E(2807,!1);return c.body}));case"text":return a.pipe(Q(c=>{if(c.body!==null&&typeof c.body!="string")throw new E(2808,!1);return c.body}));default:return a.pipe(Q(c=>c.body))}case"response":return a;default:throw new E(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Di().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Mm(r,i))}post(e,i,r={}){return this.request("POST",e,Mm(r,i))}put(e,i,r={}){return this.request("PUT",e,Mm(r,i))}static \u0275fac=function(i){return new(i||t)(M(km))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var gR=new _("",{factory:()=>!0}),vR="XSRF-TOKEN",yR=new _("",{factory:()=>vR}),_R="X-XSRF-TOKEN",bR=new _("",{factory:()=>_R}),CR=(()=>{class t{cookieName=u(yR);doc=u(V);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Ia(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),s0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(CR),r},providedIn:"root"})}return t})();function ER(t,n){if(!u(gR)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=u(Ko).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch(r){return n(t)}let e=u(s0).getToken(),i=u(bR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var Om=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t[t.Xhr=7]="Xhr",t})(Om||{});function wR(t,n){return{\u0275kind:t,\u0275providers:n}}function Fm(...t){let n=[Cn,Hd,zd,{provide:km,useExisting:zd},{provide:Nm,useFactory:()=>u(Hd)},{provide:Rm,useValue:ER,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return ui(n)}function Pm(t){return wR(Om.Interceptors,t.map(n=>({provide:Rm,useValue:n,multi:!0})))}var a0=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(M(V))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ka=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(DR),r},providedIn:"root"})}return t})(),DR=(()=>{class t extends ka{_doc=u(V);sanitize(e,i){if(i==null)return null;switch(e){case nt.NONE:return i;case nt.HTML:return bi(i,"HTML")?kn(i):xp(this._doc,String(i)).toString();case nt.STYLE:return bi(i,"Style")?kn(i):i;case nt.SCRIPT:if(bi(i,"Script"))return kn(i);throw new E(5200,!1);case nt.URL:return bi(i,"URL")?kn(i):pa(String(i));case nt.RESOURCE_URL:if(bi(i,"ResourceURL"))return kn(i);throw new E(5201,!1);default:throw new E(5202,!1)}}bypassSecurityTrustHtml(e){return bp(e)}bypassSecurityTrustStyle(e){return Cp(e)}bypassSecurityTrustScript(e){return Ep(e)}bypassSecurityTrustUrl(e){return wp(e)}bypassSecurityTrustResourceUrl(e){return Dp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Y="primary",qa=Symbol("RouteTitle"),Um=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function qr(t){return new Um(t)}function Lm(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function v0(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return Lm(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!Lm(o,t.slice(0,o.length),a)||!Lm(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function Zd(t){return new Promise((n,e)=>{t.pipe(ai()).subscribe({next:i=>n(i),error:i=>e(i)})})}function xR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Qn(t[e],n[e]))return!1;return!0}function Qn(t,n){let e=t?Hm(t):void 0,i=n?Hm(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!y0(t[r],n[r]))return!1;return!0}function Hm(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function y0(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function IR(t){return t.length>0?t[t.length-1]:null}function Kr(t){return Vs(t)?t:qi(t)?Ae(Promise.resolve(t)):L(t)}function _0(t){return Vs(t)?Zd(t):Promise.resolve(t)}var SR={exact:E0,subset:w0},b0={exact:MR,subset:TR,ignored:()=>!0},C0={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},zm={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function l0(t,n,e){return SR[e.paths](t.root,n.root,e.matrixParams)&&b0[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function MR(t,n){return Qn(t,n)}function E0(t,n,e){if(!Wr(t.segments,n.segments)||!Wd(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!E0(t.children[i],n.children[i],e))return!1;return!0}function TR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>y0(t[e],n[e]))}function w0(t,n,e){return D0(t,n,n.segments,e)}function D0(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Wr(r,e)||n.hasChildren()||!Wd(r,e,i))}else if(t.segments.length===e.length){if(!Wr(t.segments,e)||!Wd(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!w0(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Wr(t.segments,r)||!Wd(t.segments,r,i)||!t.children[Y]?!1:D0(t.children[Y],n,o,i)}}function Wd(t,n,e){return n.every((i,r)=>b0[e](t[r].parameters,i.parameters))}var tn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ge([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=qr(this.queryParams),this._queryParamMap}toString(){return NR.serialize(this)}},ge=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return qd(this)}},Ji=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=qr(this.parameters),this._parameterMap}toString(){return I0(this)}};function AR(t,n){return Wr(t,n)&&t.every((e,i)=>Qn(e.parameters,n[i].parameters))}function Wr(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function RR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===Y&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==Y&&(e=e.concat(n(r,i)))}),e}var ls=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>new er})}return t})(),er=class{parse(n){let e=new Gm(n);return new tn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Oa(n.root,!0)}`,i=FR(n.queryParams),r=typeof n.fragment=="string"?`#${kR(n.fragment)}`:"";return`${e}${i}${r}`}},NR=new er;function qd(t){return t.segments.map(n=>I0(n)).join("/")}function Oa(t,n){if(!t.hasChildren())return qd(t);if(n){let e=t.children[Y]?Oa(t.children[Y],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==Y&&i.push(`${r}:${Oa(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=RR(t,(i,r)=>r===Y?[Oa(t.children[Y],!1)]:[`${r}:${Oa(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[Y]!=null?`${qd(t)}/${e[0]}`:`${qd(t)}/(${e.join("//")})`}}function x0(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function $d(t){return x0(t).replace(/%3B/gi,";")}function kR(t){return encodeURI(t)}function $m(t){return x0(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Yd(t){return decodeURIComponent(t)}function d0(t){return Yd(t.replace(/\+/g,"%20"))}function I0(t){return`${$m(t.path)}${OR(t.parameters)}`}function OR(t){return Object.entries(t).map(([n,e])=>`;${$m(n)}=${$m(e)}`).join("")}function FR(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${$d(e)}=${$d(r)}`).join("&"):`${$d(e)}=${$d(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var PR=/^[^\/()?;#]+/;function Vm(t){let n=t.match(PR);return n?n[0]:""}var LR=/^[^\/()?;=#]+/;function VR(t){let n=t.match(LR);return n?n[0]:""}var jR=/^[^=?&#]+/;function BR(t){let n=t.match(jR);return n?n[0]:""}var UR=/^[^&#]+/;function HR(t){let n=t.match(UR);return n?n[0]:""}var Gm=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ge([],{}):new ge([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new E(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[Y]=new ge(e,i)),r}parseSegment(){let n=Vm(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new E(4009,!1);return this.capture(n),new Ji(Yd(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=VR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Vm(this.remaining);r&&(i=r,this.capture(i))}n[Yd(e)]=Yd(i)}parseQueryParam(n){let e=BR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=HR(this.remaining);s&&(i=s,this.capture(i))}let r=d0(e),o=d0(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Vm(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new E(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=Y);let a=this.parseChildren(e+1);i[s??Y]=Object.keys(a).length===1&&a[Y]?a[Y]:new ge([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new E(4011,!1)}};function S0(t){return t.segments.length>0?new ge([],{[Y]:t}):t}function M0(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=M0(r);if(i===Y&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new ge(t.segments,n);return zR(e)}function zR(t){if(t.numberOfChildren===1&&t.children[Y]){let n=t.children[Y];return new ge(t.segments.concat(n.segments),n.children)}return t}function tr(t){return t instanceof tn}function T0(t,n,e=null,i=null,r=new er){let o=A0(t);return R0(o,n,e,i,r)}function A0(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new ge(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=S0(i);return n??r}function R0(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return jm(o,o,o,e,i,r);let s=$R(n);if(s.toRoot())return jm(o,o,new ge([],{}),e,i,r);let a=GR(s,o,t),c=a.processChildren?Pa(a.segmentGroup,a.index,s.commands):k0(a.segmentGroup,a.index,s.commands);return jm(o,a.segmentGroup,c,e,i,r)}function Kd(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function ja(t){return typeof t=="object"&&t!=null&&t.outlets}function u0(t,n,e){t||="\u0275";let i=new tn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function jm(t,n,e,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(f=>u0(l,f,o)):u0(l,d,o);let a;t===n?a=e:a=N0(t,n,e);let c=S0(M0(a));return new tn(c,s,r)}function N0(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=N0(o,n,e)}),new ge(t.segments,i)}var Qd=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Kd(i[0]))throw new E(4003,!1);let r=i.find(ja);if(r&&r!==IR(i))throw new E(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function $R(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Qd(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Qd(e,n,i)}var ns=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function GR(t,n,e){if(t.isAbsolute)return new ns(n,!0,0);if(!e)return new ns(n,!1,NaN);if(e.parent===null)return new ns(e,!0,0);let i=Kd(t.commands[0])?0:1,r=e.segments.length-1+i;return WR(e,r,t.numberOfDoubleDots)}function WR(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new E(4005,!1);r=i.segments.length}return new ns(i,!1,r-o)}function qR(t){return ja(t[0])?t[0].outlets:{[Y]:t}}function k0(t,n,e){if(t??=new ge([],{}),t.segments.length===0&&t.hasChildren())return Pa(t,n,e);let i=YR(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new ge(t.segments.slice(0,i.pathIndex),{});return o.children[Y]=new ge(t.segments.slice(i.pathIndex),t.children),Pa(o,0,r)}else return i.match&&r.length===0?new ge(t.segments,{}):i.match&&!t.hasChildren()?Wm(t,n,e):i.match?Pa(t,0,r):Wm(t,n,e)}function Pa(t,n,e){if(e.length===0)return new ge(t.segments,{});{let i=qR(e),r={};if(Object.keys(i).some(o=>o!==Y)&&t.children[Y]&&t.numberOfChildren===1&&t.children[Y].segments.length===0){let o=Pa(t.children[Y],n,e);return new ge(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=k0(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new ge(t.segments,r)}}function YR(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(ja(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!h0(c,l,s))return o;i+=2}else{if(!h0(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Wm(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(ja(o)){let c=ZR(o.outlets);return new ge(i,c)}if(r===0&&Kd(e[0])){let c=t.segments[n];i.push(new Ji(c.path,f0(e[0]))),r++;continue}let s=ja(o)?o.outlets[Y]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&Kd(a)?(i.push(new Ji(s,f0(a))),r+=2):(i.push(new Ji(s,{})),r++)}return new ge(i,{})}function ZR(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Wm(new ge([],{}),0,i))}),n}function f0(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function h0(t,n,e){return t==e.path&&Qn(n,e.parameters)}var La="imperative",lt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(lt||{}),nn=class{id;url;constructor(n,e){this.id=n,this.url=e}},Yr=class extends nn{type=lt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Xn=class extends nn{urlAfterRedirects;type=lt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},bt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(bt||{}),Ba=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Ba||{}),En=class extends nn{reason;code;type=lt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function O0(t){return t instanceof En&&(t.code===bt.Redirect||t.code===bt.SupersededByNewNavigation)}var Si=class extends nn{reason;code;type=lt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},Zr=class extends nn{error;target;type=lt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Ua=class extends nn{urlAfterRedirects;state;type=lt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Xd=class extends nn{urlAfterRedirects;state;type=lt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Jd=class extends nn{urlAfterRedirects;state;shouldActivate;type=lt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},eu=class extends nn{urlAfterRedirects;state;type=lt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},tu=class extends nn{urlAfterRedirects;state;type=lt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},nu=class{route;type=lt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},iu=class{route;type=lt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},ru=class{snapshot;type=lt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ou=class{snapshot;type=lt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},su=class{snapshot;type=lt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},au=class{snapshot;type=lt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var rs=class{},Ha=class{},os=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function KR(t){return!(t instanceof rs)&&!(t instanceof os)&&!(t instanceof Ha)}var cu=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ds(this.rootInjector)}},ds=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new cu(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(M(be))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),lu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=qm(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=qm(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Ym(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Ym(n,this._root).map(e=>e.value)}};function qm(t,n){if(t===n.value)return n;for(let e of n.children){let i=qm(t,e);if(i)return i}return null}function Ym(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Ym(t,e);if(i.length)return i.unshift(n),i}return[]}var en=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function ts(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var za=class extends lu{snapshot;constructor(n,e){super(n),this.snapshot=e,ig(this,n)}toString(){return this.snapshot.toString()}};function F0(t,n){let e=QR(t,n),i=new et([new Ji("",{})]),r=new et({}),o=new et({}),s=new et({}),a=new et(""),c=new rn(i,r,s,a,o,Y,t,e.root);return c.snapshot=e.root,new za(new en(c,[]),e)}function QR(t,n){let e={},i={},r={},s=new ss([],e,r,"",i,Y,t,null,{},n);return new $a("",new en(s,[]))}var rn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Q(l=>l[qa]))??L(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Q(n=>qr(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Q(n=>qr(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},XR="always";function ng(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&L0(r)&&(i.resolve[qa]=r.title),i}var ss=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[qa]}constructor(n,e,i,r,o,s,a,c,l,d){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=qr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=qr(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},$a=class extends lu{url;constructor(n,e){super(e),this.url=n,ig(this,e)}toString(){return P0(this._root)}};function ig(t,n){n.value._routerState=t,n.children.forEach(e=>ig(t,e))}function P0(t){let n=t.children.length>0?` { ${t.children.map(P0).join(", ")} } `:"";return`${t.value}${n}`}function Bm(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Qn(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Qn(n.params,e.params)||t.paramsSubject.next(e.params),xR(n.url,e.url)||t.urlSubject.next(e.url),Qn(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Zm(t,n){let e=Qn(t.params,n.params)&&AR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Zm(t.parent,n.parent))}function L0(t){return typeof t.title=="string"||t.title===null}var V0=new _(""),Ya=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Y;activateEvents=new X;deactivateEvents=new X;attachEvents=new X;detachEvents=new X;routerOutletData=Zo();parentContexts=u(ds);location=u(Pt);changeDetector=u(me);inputBinder=u(hu,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new E(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new E(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new E(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new E(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Km(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ye]})}return t})(),Km=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===rn?this.route:n===ds?this.childContexts:n===V0?this.outletData:this.parent.get(n,e)}},hu=new _("");var rg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&ue(0,"router-outlet")},dependencies:[Ya],encapsulation:2,changeDetection:1})}return t})();function og(t){let n=t.children&&t.children.map(og),e=n?j(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==Y&&(e.component=rg),e}function JR(t,n,e){let i=new Set,r=Ga(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new za(r,n)}}function Ga(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=eN(t,n,e,i);return new en(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(c=>Ga(t,c,void 0,i)),a}}let r=tN(n.value);i.add(r);let o=n.children.map(s=>Ga(t,s,void 0,i));return new en(r,o)}}function eN(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return Ga(t,r,o,i);return Ga(t,r,void 0,i)})}function tN(t){return new rn(new et(t.url),new et(t.params),new et(t.queryParams),new et(t.fragment),new et(t.data),t.outlet,t.component,t)}var as=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},j0="ngNavigationCancelingError";function du(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=tr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=B0(!1,bt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function B0(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[j0]=!0,e.cancellationCode=n,e}function nN(t){return U0(t)&&tr(t.url)}function U0(t){return!!t&&t[j0]}var Qm=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Bm(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=ts(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ts(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ts(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=ts(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new au(o.value.snapshot))}),n.children.length&&this.forwardEvent(new ou(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Bm(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Bm(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},uu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},is=class{component;route;constructor(n,e){this.component=n,this.route=e}};function iN(t,n,e){let i=t._root,r=n?n._root:null;return Fa(i,r,e,[i.value])}function rN(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function us(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!$f(t)?t:n.get(t):i}function Fa(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ts(n);return t.children.forEach(s=>{oN(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Va(a,e.getContext(s),r)),r}function oN(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=sN(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new uu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Fa(t,n,a?a.children:null,i,r):Fa(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new is(a.outlet.component,s))}else s&&Va(n,a,r),r.canActivateChecks.push(new uu(i)),o.component?Fa(t,null,a?a.children:null,i,r):Fa(t,null,e,i,r);return r}function sN(t,n,e){if(typeof e=="function")return st(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Wr(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Wr(t.url,n.url)||!Qn(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Zm(t,n)||!Qn(t.queryParams,n.queryParams);default:return!Zm(t,n)}}function Va(t,n,e){let i=ts(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?Va(s,n.children.getContext(o),e):Va(s,null,e):Va(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new is(n.outlet.component,r)):e.canDeactivateChecks.push(new is(null,r)):e.canDeactivateChecks.push(new is(null,r))}function Za(t){return typeof t=="function"}function aN(t){return typeof t=="boolean"}function cN(t){return t&&Za(t.canLoad)}function lN(t){return t&&Za(t.canActivate)}function dN(t){return t&&Za(t.canActivateChild)}function uN(t){return t&&Za(t.canDeactivate)}function fN(t){return t&&Za(t.canMatch)}function H0(t){return t instanceof _r||t?.name==="EmptyError"}var Gd=Symbol("INITIAL_VALUE");function cs(){return tt(t=>If(t.map(n=>n.pipe(Yt(1),Zt(Gd)))).pipe(Q(n=>{for(let e of n)if(e!==!0){if(e===Gd)return Gd;if(e===!1||hN(e))return e}return!0}),Me(n=>n!==Gd),Yt(1)))}function hN(t){return tr(t)||t instanceof as}function z0(t){return t.aborted?L(void 0).pipe(Yt(1)):new ee(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function $0(t){return Ne(z0(t))}function pN(t){return St(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?L(j(C({},n),{guardsResult:!0})):mN(o,e,i).pipe(St(s=>s&&aN(s)?gN(e,r,t):L(s)),Q(s=>j(C({},n),{guardsResult:s})))})}function mN(t,n,e){return Ae(t).pipe(St(i=>CN(i.component,i.route,e,n)),ai(i=>i!==!0,!0))}function gN(t,n,e){return Ae(n).pipe(Io(i=>xo(yN(i.route.parent,e),vN(i.route,e),bN(t,i.path),_N(t,i.route))),ai(i=>i!==!0,!0))}function vN(t,n){return t!==null&&n&&n(new su(t)),L(!0)}function yN(t,n){return t!==null&&n&&n(new ru(t)),L(!0)}function _N(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return L(!0);let i=e.map(r=>br(()=>{let o=n._environmentInjector,s=us(r,o),a=lN(s)?s.canActivate(n,t):st(o,()=>s(n,t));return Kr(a).pipe(ai())}));return L(i).pipe(cs())}function bN(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>rN(o)).filter(o=>o!==null).map(o=>br(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=us(a,c),d=dN(l)?l.canActivateChild(e,t):st(c,()=>l(e,t));return Kr(d).pipe(ai())});return L(s).pipe(cs())}));return L(r).pipe(cs())}function CN(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return L(!0);let o=r.map(s=>{let a=n._environmentInjector,c=us(s,a),l=uN(c)?c.canDeactivate(t,n,e,i):st(a,()=>c(t,n,e,i));return Kr(l).pipe(ai())});return L(o).pipe(cs())}function EN(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return L(!0);let s=o.map(a=>{let c=us(a,t),l=cN(c)?c.canLoad(n,e):st(t,()=>c(n,e)),d=Kr(l);return r?d.pipe($0(r)):d});return L(s).pipe(cs(),G0(i))}function G0(t){return Ef(He(n=>{if(typeof n!="boolean")throw du(t,n)}),Q(n=>n===!0))}function wN(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return L(!0);let a=s.map(c=>{let l=us(c,t),d=fN(l)?l.canMatch(n,e,r):st(t,()=>l(n,e,r));return Kr(d).pipe($0(o))});return L(a).pipe(cs(),G0(i))}var Ii=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Wa=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function DN(t){throw new E(4e3,!1)}function xN(t){throw B0(!1,bt.GuardRejected)}var Xm=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}lineralizeSegments(n,e){return _e(this,null,function*(){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Y])throw DN(`${n.redirectTo}`);r=r.children[Y]}})}applyRedirectCommands(n,e,i,r,o){return _e(this,null,function*(){let s=yield IN(e,r,o);if(s instanceof tn)throw new Wa(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Wa(a);return a})}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new tn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new ge(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new E(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function IN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Zd(Kr(st(e,()=>i(n))))}function SN(t,n){return t.providers&&!t._injector&&(t._injector=va(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Fn(t){return t.outlet||Y}function MN(t,n){let e=t.filter(i=>Fn(i)===n);return e.push(...t.filter(i=>Fn(i)!==n)),e}var Jm={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function W0(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function TN(t,n,e,i,r,o,s){let a=q0(t,n,e);if(!a.matched)return L(a);let c=W0(o(a));return i=SN(n,i),wN(i,n,e,r,c,s).pipe(Q(l=>l===!0?a:C({},Jm)))}function q0(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},Jm):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||v0)(e,t,n);if(!r)return C({},Jm);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function p0(t,n,e,i,r){return e.length>0&&NN(t,e,i,r)?{segmentGroup:new ge(n,RN(i,new ge(e,t.children))),slicedSegments:[]}:e.length===0&&kN(t,e,i)?{segmentGroup:new ge(t.segments,AN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new ge(t.segments,t.children),slicedSegments:e}}function AN(t,n,e,i){let r={};for(let o of e)if(pu(t,n,o)&&!i[Fn(o)]){let s=new ge([],{});r[Fn(o)]=s}return C(C({},i),r)}function RN(t,n){let e={};e[Y]=n;for(let i of t)if(i.path===""&&Fn(i)!==Y){let r=new ge([],{});e[Fn(i)]=r}return e}function NN(t,n,e,i){return e.some(r=>!pu(t,n,r)||!(Fn(r)!==Y)?!1:!(i!==void 0&&Fn(r)===i))}function kN(t,n,e){return e.some(i=>pu(t,n,i))}function pu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function ON(t,n,e){return n.length===0&&!t.children[e]}var eg=class{};function FN(t,n,e,i,r,o,s,a){return _e(this,null,function*(){return new tg(t,n,e,i,r,s,o,a).recognize()})}var PN=31,tg=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Xm(this.urlSerializer,this.urlTree)}noMatchError(n){return new E(4002,`'${n.segmentGroup}'`)}recognize(){return _e(this,null,function*(){let n=p0(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=yield this.match(n),r=new en(i,e),o=new $a("",r),s=T0(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}})}match(n){return _e(this,null,function*(){let e=new ss([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Y,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,n,Y,e),rootSnapshot:e}}catch(i){if(i instanceof Wa)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ii?this.noMatchError(i):i}})}processSegmentGroup(n,e,i,r,o){return _e(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=yield this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof en?[s]:[]})}processChildren(n,e,i,r){return _e(this,null,function*(){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=MN(e,c),f=yield this.processSegmentGroup(n,d,l,c,r);s.push(...f)}let a=Y0(s);return LN(a),a})}processSegment(n,e,i,r,o,s,a){return _e(this,null,function*(){for(let c of e)try{return yield this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof Ii||H0(l))continue;throw l}if(ON(i,r,o))return new eg;throw new Ii(i)})}processSegmentAgainstRoute(n,e,i,r,o,s,a,c){return _e(this,null,function*(){if(Fn(i)!==s&&(s===Y||!pu(r,o,i)))throw new Ii(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new Ii(r)})}expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){return _e(this,null,function*(){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:h}=q0(e,r,o);if(!c)throw new Ii(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>PN&&(this.allowRedirects=!1));let g=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let b=yield this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,W0(g),n),w=yield this.applyRedirects.lineralizeSegments(r,b);return this.processSegment(n,i,e,w.concat(h),s,!1,a)})}createSnapshot(n,e,i,r,o){let s=new ss(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,jN(e),Fn(e),e.component??e._loadedComponent??null,e,BN(e),n),a=ng(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}matchSegmentAgainstRoute(n,e,i,r,o,s){return _e(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=vt=>this.createSnapshot(n,i,vt.consumedSegments,vt.parameters,s),c=yield Zd(TN(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new Ii(e);n=i._injector??n;let{routes:l}=yield this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:g}=c,b=this.createSnapshot(n,i,h,f,s),{segmentGroup:w,slicedSegments:R}=p0(e,h,g,l,o);if(R.length===0&&w.hasChildren()){let vt=yield this.processChildren(d,l,w,b);return new en(b,vt)}if(l.length===0&&R.length===0)return new en(b,[]);let J=Fn(i)===o,Je=yield this.processSegment(d,l,w,R,J?Y:o,!0,b);return new en(b,Je instanceof en?[Je]:[])})}getChildConfig(n,e,i){return _e(this,null,function*(){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield Zd(EN(n,e,i,this.urlSerializer,this.abortSignal))){let o=yield this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw xN(e)}return{routes:[],injector:n}})}};function LN(t){t.sort((n,e)=>n.value.outlet===Y?-1:e.value.outlet===Y?1:n.value.outlet.localeCompare(e.value.outlet))}function VN(t){let n=t.value.routeConfig;return n&&n.path===""}function Y0(t){let n=[],e=new Set;for(let i of t){if(!VN(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=Y0(i.children);n.push(new en(i.value,r))}return n.filter(i=>!e.has(i))}function jN(t){return t.data||{}}function BN(t){return t.resolve||{}}function UN(t,n,e,i,r,o,s){return St(a=>_e(null,null,function*(){let{state:c,tree:l}=yield FN(t,n,e,i,a.extractedUrl,r,o,s);return j(C({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function HN(t){return St(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return L(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of Z0(a))o.add(c);let s=0;return Ae(o).pipe(Io(a=>r.has(a)?zN(a,e,t):(a.data=ng(a,a.parent,t).resolve,L(void 0))),He(()=>s++),cl(1),St(a=>s===o.size?L(n):Ue))})}function Z0(t){let n=t.children.map(e=>Z0(e)).flat();return[t,...n]}function zN(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!L0(i)&&(r[qa]=i.title),br(()=>(t.data=ng(t,t.parent,e).resolve,$N(r,t,n).pipe(Q(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function $N(t,n,e){let i=Hm(t);if(i.length===0)return L({});let r={};return Ae(i).pipe(St(o=>GN(t[o],n,e).pipe(ai(),He(s=>{if(s instanceof as)throw du(new er,s);r[o]=s}))),cl(1),Q(()=>r),Cr(o=>H0(o)?Ue:Ls(o)))}function GN(t,n,e){let i=n._environmentInjector,r=us(t,i),o=r.resolve?r.resolve(n,e):st(i,()=>r(n,e));return Kr(o)}function m0(t){return tt(n=>{let e=t(n);return e?Ae(e).pipe(Q(()=>n)):L(n)})}var sg=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===Y);return i}getResolvedTitleForRoute(e){return e.data[qa]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(K0)})}return t})(),K0=(()=>{class t extends sg{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(M(a0))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),fs=new _("",{factory:()=>({})}),Ka=new _(""),Q0=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(am);loadComponent(e,i){return _e(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=_e(this,null,function*(){try{let o=yield _0(st(e,()=>i.loadComponent())),s=yield J0(gm(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=_e(this,null,function*(){try{let o=yield X0(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function X0(t,n,e,i){return _e(this,null,function*(){let r=yield _0(st(e,()=>t.loadChildren())),o=yield J0(gm(r)),s;o instanceof Cd||Array.isArray(o)?s=o:s=yield n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,d=s,c=a.get(Ka,[],{optional:!0,self:!0}).flat()),{routes:c.map(og),injector:a,factory:d}})}function J0(t){return _e(this,null,function*(){return t})}var mu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(WN)})}return t})(),WN=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),eE=new _("");var tE=new _(""),qN=()=>{},nE=new _(""),iE=(()=>{class t{currentNavigation=ie(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=ie(null);events=new I;transitionAbortWithErrorSubject=new I;configLoader=u(Q0);environmentInjector=u(be);destroyRef=u(ht);urlSerializer=u(ls);rootContexts=u(ds);location=u(Xi);inputBindingEnabled=u(hu,{optional:!0})!==null;titleStrategy=u(sg);options=u(fs,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||XR;urlHandlingStrategy=u(mu);createViewTransition=u(eE,{optional:!0});navigationErrorHandler=u(nE,{optional:!0});activatedRouteInjectorFeature=u(tE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>L(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new nu(r)),i=r=>this.events.next(new iu(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;fe(()=>{this.transitions?.next(j(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new et(null),this.transitions.pipe(Me(i=>i!==null),tt(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return L(i).pipe(tt(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",bt.SupersededByNewNavigation),Ue;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?j(C({},l),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=c.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new Si(c.id,this.urlSerializer.serialize(c.rawUrl),"",Ba.IgnoredSameUrlNavigation)),c.resolve(!1),Ue;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return L(c).pipe(tt(h=>(this.events.next(new Yr(h.id,this.urlSerializer.serialize(h.extractedUrl),h.source,h.restoredState)),h.id!==this.navigationId?Ue:Promise.resolve(h))),UN(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),He(h=>{i.targetSnapshot=h.targetSnapshot,i.urlAfterRedirects=h.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=h.urlAfterRedirects,g)),this.events.next(new Ha)}),tt(h=>Ae(i.routesRecognizeHandler.deferredHandle??L(void 0)).pipe(Q(()=>h))),He(()=>{let h=new Ua(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(h)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:h,extractedUrl:g,source:b,restoredState:w,extras:R}=c,J=new Yr(h,this.urlSerializer.serialize(g),b,w);this.events.next(J);let Je=F0(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=j(C({},c),{targetSnapshot:Je,urlAfterRedirects:g,extras:j(C({},R),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(vt=>(vt.finalUrl=g,vt)),L(i)}else return this.events.next(new Si(c.id,this.urlSerializer.serialize(c.extractedUrl),"",Ba.IgnoredByUrlHandlingStrategy)),c.resolve(!1),Ue}),Q(c=>{let l=new Xd(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=j(C({},c),{guards:iN(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),pN(c=>this.events.next(c)),tt(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw du(this.urlSerializer,c.guardsResult);let l=new Jd(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!a())return Ue;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",bt.GuardRejected),Ue;if(c.guards.canActivateChecks.length===0)return L(c);let d=new eu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!a())return Ue;let f=!1;return L(c).pipe(HN(this.paramsInheritanceStrategy),He({next:()=>{f=!0;let h=new tu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(h)},complete:()=>{f||this.cancelNavigationTransition(c,"",bt.NoDataFromResolver)}}))}),m0(c=>{let l=f=>{let h=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let g=f._environmentInjector;h.push(this.configLoader.loadComponent(g,f.routeConfig).then(b=>{f.component=b}))}for(let g of f.children)h.push(...l(g));return h},d=l(c.targetSnapshot.root);return d.length===0?L(c):Ae(Promise.all(d).then(()=>c))}),tt(c=>{let{newlyCreatedRoutes:l,state:d}=JR(e.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=j(C({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),L(c)}),this.activatedRouteInjectorFeature?.operator()??(c=>c),m0(()=>this.afterPreactivation()),tt(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return d?Ae(d).pipe(Q(()=>i)):L(i)}),Yt(1),tt(c=>{r=!1,this.events.next(new rs);let l=i.beforeActivateHandler.deferredHandle;return l?Ae(l.then(()=>c)):L(c)}),He(c=>{new Qm(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(l=>(l.abort=qN,l)),this.lastSuccessfulNavigation.set(fe(this.currentNavigation)),this.events.next(new Xn(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),Ne(z0(s.signal).pipe(Me(()=>!o&&r),He(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",bt.Aborted)}))),He({complete:()=>{o=!0}}),Ne(this.transitionAbortWithErrorSubject.pipe(He(c=>{throw c}))),Er(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",bt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Cr(c=>{if(o=!0,g0(i),this.destroyed)return i.resolve(!1),Ue;if(U0(c))this.events.next(new En(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),nN(c)?this.events.next(new os(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new Zr(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let d=st(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof as){let{message:f,cancellationCode:h}=du(this.urlSerializer,d);this.events.next(new En(i.id,this.urlSerializer.serialize(i.extractedUrl),f,h)),this.events.next(new os(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return Ue}))}))}cancelNavigationTransition(e,i,r){g0(e);let o=new En(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=fe(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function YN(t){return t!==La}function g0(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var rE=new _("");var oE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(ZN)})}return t})(),fu=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},ZN=(()=>{class t extends fu{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),gu=(()=>{class t{urlSerializer=u(ls);options=u(fs,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(Xi);urlHandlingStrategy=u(mu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new tn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof tn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=F0(null,u(be));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(KN)})}return t})(),KN=(()=>{class t extends gu{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Yr?this.updateStateMemento():e instanceof Si?this.commitTransition(i):e instanceof Ua?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof rs?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof En&&!O0(e)?this.restoreHistory(i):e instanceof Zr?this.restoreHistory(i,!0):e instanceof Xn&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=C(C({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=C(C({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function ag(t,n){t.events.pipe(Me(e=>e instanceof Xn||e instanceof En||e instanceof Zr||e instanceof Si),Q(e=>e instanceof Xn||e instanceof Si?0:(e instanceof En?e.code===bt.Redirect||e.code===bt.SupersededByNewNavigation:!1)?2:1),Me(e=>e!==2),Yt(1)).subscribe(()=>{n()})}var Xe=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(Ed);stateManager=u(gu);options=u(fs,{optional:!0})||{};pendingTasks=u(gi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(iE);urlSerializer=u(ls);location=u(Xi);urlHandlingStrategy=u(mu);injector=u(be);_events=new I;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(oE);injectorCleanup=u(rE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(Ka,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(hu,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new de;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=fe(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof En&&i.code!==bt.Redirect&&i.code!==bt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Xn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof os){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||YN(r.source)},s);this.scheduleNavigation(a,La,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}KR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),La,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=j(C({},o),{browserUrl:e})),r){let l=C({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Xt)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return fe(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(og),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=A0(h)}catch(h){(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return R0(f,e,d,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=tr(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,La,null,i)}navigate(e,i={skipLocationChange:!1}){return QN(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch(i){return this.console.warn(di(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},C0):i===!1?r=C({},zm):r=C(C({},zm),i),tr(e))return l0(this.currentUrlTree,e,r);let o=this.parseUrl(e);return l0(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((f,h)=>{a=f,c=h});let d=this.pendingTasks.add();return ag(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function QN(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new E(4008,!1)}var ek=(()=>{class t{router=u(Xe);stateManager=u(gu);fragment=ie("");queryParams=ie({});path=ie("");serializer=u(ls);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Xn&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new tn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Ct=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=u(new Zi("href"),{optional:!0});reactiveHref=cm(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return fe(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return fe(this._target)}_target=ie(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return fe(this._queryParams)}_queryParams=ie(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return fe(this._fragment)}_fragment=ie(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return fe(this._queryParamsHandling)}_queryParamsHandling=ie(void 0);set state(e){this._state.set(e)}get state(){return fe(this._state)}_state=ie(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return fe(this._info)}_info=ie(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return fe(this._relativeTo)}_relativeTo=ie(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return fe(this._preserveFragment)}_preserveFragment=ie(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return fe(this._skipLocationChange)}_skipLocationChange=ie(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return fe(this._replaceUrl)}_replaceUrl=ie(!1);browserUrl=Zo(void 0);isAnchorElement;onChanges=new I;applicationErrorHandler=u(Xt);options=u(fs,{optional:!0});reactiveRouterState=u(ek);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=ie(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(tr(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=C({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(a,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Rt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:tr(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return fe(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(D(Xe),D(rn),ha("tabindex"),D($e),D(H),D(Qo))};static \u0275dir=P({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&$("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&pe("href",r.reactiveHref(),Ip)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",W],skipLocationChange:[2,"skipLocationChange","skipLocationChange",W],replaceUrl:[2,"replaceUrl","replaceUrl",W],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[Ye]})}return t})();var tk=new _("");function cg(t,...n){return ui([{provide:Ka,multi:!0,useValue:t},{provide:rn,useFactory:nk},{provide:ya,multi:!0,useFactory:ik},n.map(e=>e.\u0275providers)])}function nk(){return u(Xe).routerState.root}function ik(){let t=u(ce);return n=>{let e=t.get(Jt);if(n!==e.components[0])return;let i=t.get(Xe),r=t.get(rk);t.get(ok)===1&&i.initialNavigation(),t.get(sk,null,{optional:!0})?.setUpPreloading(),t.get(tk,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var rk=new _("",{factory:()=>new I}),ok=new _("",{factory:()=>1});var sk=new _("");var pE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(D($e),D(H))};static \u0275dir=P({type:t})}return t})(),ak=(()=>{class t extends pE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Wn(t)))(r||t)}})();static \u0275dir=P({type:t,features:[Ze]})}return t})(),mE=new _("");var ck={provide:mE,useExisting:cn(()=>Et),multi:!0};function lk(){let t=_n()?_n().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var dk=new _(""),Et=(()=>{class t extends pE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!lk())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(D($e),D(H),D(dk,8))};static \u0275dir=P({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&$("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Qe([ck]),Ze]})}return t})();function ug(t){return t==null||fg(t)===0}function fg(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Mu=new _(""),hg=new _(""),uk=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Qr=class{static min(n){return fk(n)}static max(n){return hk(n)}static required(n){return gE(n)}static requiredTrue(n){return pk(n)}static email(n){return mk(n)}static minLength(n){return gk(n)}static maxLength(n){return vk(n)}static pattern(n){return yk(n)}static nullValidator(n){return yu()}static compose(n){return EE(n)}static composeAsync(n){return wE(n)}};function fk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function hk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function gE(t){return ug(t.value)?{required:!0}:null}function pk(t){return t.value===!0?null:{required:!0}}function mk(t){return ug(t.value)||uk.test(t.value)?null:{email:!0}}function gk(t){return n=>{let e=n.value?.length??fg(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function vk(t){return n=>{let e=n.value?.length??fg(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function yk(t){if(!t)return yu;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(ug(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function yu(t){return null}function vE(t){return t!=null}function yE(t){return qi(t)?Ae(t):t}function _E(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function bE(t,n){return n.map(e=>e(t))}function _k(t){return!t.validate}function CE(t){return t.map(n=>_k(n)?n:e=>n.validate(e))}function EE(t){if(!t)return null;let n=t.filter(vE);return n.length==0?null:function(e){return _E(bE(e,n))}}function pg(t){return t!=null?EE(CE(t)):null}function wE(t){if(!t)return null;let n=t.filter(vE);return n.length==0?null:function(e){let i=bE(e,n).map(yE);return js(i).pipe(Q(_E))}}function mg(t){return t!=null?wE(CE(t)):null}function sE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function DE(t){return t._rawValidators}function xE(t){return t._rawAsyncValidators}function lg(t){return t?Array.isArray(t)?t:[t]:[]}function _u(t,n){return Array.isArray(t)?t.includes(n):t===n}function aE(t,n){let e=lg(n);return lg(t).forEach(r=>{_u(e,r)||e.push(r)}),e}function cE(t,n){return lg(n).filter(e=>!_u(t,e))}var bu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=pg(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=mg(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},nr=class extends bu{name;get formDirective(){return null}get path(){return null}};var Qa="VALID",vu="INVALID",ps="PENDING",Xa="DISABLED",ir=class{},Cu=class extends ir{value;source;constructor(n,e){super(),this.value=n,this.source=e}},ec=class extends ir{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},tc=class extends ir{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},ms=class extends ir{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Eu=class extends ir{source;constructor(n){super(),this.source=n}},gs=class extends ir{source;constructor(n){super(),this.source=n}};function IE(t){return(Tu(t)?t.validators:t)||null}function bk(t){return Array.isArray(t)?pg(t):t||null}function SE(t,n){return(Tu(n)?n.asyncValidators:t)||null}function Ck(t){return Array.isArray(t)?mg(t):t||null}function Tu(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function Ek(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new E(1e3,"");if(!ME(i,e))throw new E(1001,"")}function wk(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new E(-1002,"")})}var wu=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=ie(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return fe(this.statusReactive)}set status(n){fe(()=>this.statusReactive.set(n))}_status=Rt(()=>this.statusReactive());statusReactive=ie(void 0);get valid(){return this.status===Qa}get invalid(){return this.status===vu}get pending(){return this.status===ps}get disabled(){return this.status===Xa}get enabled(){return this.status!==Xa}errors;get pristine(){return fe(this.pristineReactive)}set pristine(n){fe(()=>this.pristineReactive.set(n))}_pristine=Rt(()=>this.pristineReactive());pristineReactive=ie(!0);get dirty(){return!this.pristine}get touched(){return fe(this.touchedReactive)}set touched(n){fe(()=>this.touchedReactive.set(n))}_touched=Rt(()=>this.touchedReactive());touchedReactive=ie(!1);get untouched(){return!this.touched}_events=new I;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(aE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(aE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(cE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(cE(n,this._rawAsyncValidators))}hasValidator(n){return _u(this._rawValidators,n)}hasAsyncValidator(n){return _u(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(j(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new tc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new tc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(j(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ec(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new ec(!0,i))}markAsPending(n={}){this.status=ps;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ms(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(j(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Xa,this.errors=null,this._forEachChild(r=>{r.disable(j(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Cu(this.value,i)),this._events.next(new ms(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(j(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Qa,this._forEachChild(i=>{i.enable(j(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(j(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Qa||this.status===ps)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Cu(this.value,e)),this._events.next(new ms(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(j(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Xa:Qa}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=ps,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=yE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new ms(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new X,this.statusChanges=new X}_calculateStatus(){return this._allControlsDisabled()?Xa:this.errors?vu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ps)?ps:this._anyControlsHaveStatus(vu)?vu:Qa}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new ec(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new tc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Tu(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=bk(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=Ck(this._rawAsyncValidators)}_updateHasRequiredValidator(){fe(()=>this._hasRequired.set(this.hasValidator(Qr.required)))}};function ME(t,n){return Object.hasOwn(t,n)}function Dk(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function xk(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var dg=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var Ik=(()=>{class t{_validator=yu;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):yu,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,features:[Ye]})}return t})();var Sk={provide:Mu,useExisting:cn(()=>ei),multi:!0};var ei=(()=>{class t extends Ik{required;inputName="required";normalizeInput=W;createValidator=e=>gE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Wn(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&pe("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Qe([Sk]),Ze]})}return t})();var Mk=new _(""),Au=new _("",{factory:()=>gg}),gg="always";function Tk(t,n){return[...n.path,t]}function lE(t,n,e=gg){vg(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),Rk(t,n),kk(t,n),Nk(t,n),Ak(t,n)}function dE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),xu(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Du(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function Ak(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function vg(t,n){let e=DE(t);n.validator!==null?t.setValidators(sE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=xE(t);n.asyncValidator!==null?t.setAsyncValidators(sE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Du(n._rawValidators,r),Du(n._rawAsyncValidators,r)}function xu(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=DE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=xE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Du(n._rawValidators,i),Du(n._rawAsyncValidators,i),e}function Rk(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&TE(t,n)})}function Nk(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&TE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function TE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function kk(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function AE(t,n){t==null,vg(t,n)}function Ok(t,n){return xu(t,n)}function Fk(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function Pk(t){return Object.getPrototypeOf(t.constructor)===ak}function RE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function Lk(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Et?e=o:Pk(o)?i=o:r=o}),r||i||e||null}function Vk(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var jk={provide:Mk,useFactory:()=>{let t=u(Jn,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},Jn=class extends bu{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof gs&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=Lk(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(ht)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(me);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new de,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof gs&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=Dk(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof ei))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&xk(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new dg({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=Rt(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Tn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},Iu=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Vt=(()=>{class t extends Iu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(D(Jn,2))};static \u0275dir=P({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&ae("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Ze]})}return t})(),rr=(()=>{class t extends Iu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(D(nr,10))};static \u0275dir=P({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&ae("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Ze]})}return t})(),Su=class extends wu{constructor(n,e,i){super(IE(e),SE(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){fe(()=>{wk(this,!0,n),Object.keys(n).forEach(i=>{Ek(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,j(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new gs(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return ME(this.controls,n)?this.controls[n]:null}};var Bk={provide:nr,useExisting:cn(()=>on)},Ja=Promise.resolve(),on=(()=>{class t extends nr{callSetDisabledState;get submitted(){return fe(this.submittedReactive)}_submitted=Rt(()=>this.submittedReactive());submittedReactive=ie(!1);_directives=new Set;form;ngSubmit=new X;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Su({},pg(e),mg(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Ja.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Ja.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Ja.then(()=>{let i=this._findContainer(e.path),r=new Su({});AE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Ja.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Ja.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),RE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Eu(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(D(Mu,10),D(hg,10),D(Au,8))};static \u0275dir=P({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&$("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Qe([Bk]),Ze]})}return t})();function uE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function fE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var NE=class extends wu{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(IE(e),SE(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Tu(e)&&(e.nonNullable||e.initialValueIsDefault)&&(fE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){fe(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new gs(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){uE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){uE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){fE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var Uk=t=>t instanceof NE;var Hk={provide:Jn,useExisting:cn(()=>Nt)},hE=Promise.resolve(),Nt=(()=>{class t extends Jn{_changeDetectorRef;callSetDisabledState;control=new NE;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new X;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),Fk(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,lE(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,lE(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){hE.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&W(i);hE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?Tk(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(D(nr,9),D(Mu,10),D(hg,10),D(mE,10),D(me,8),D(Au,8),D(ce,8),D($e,8))};static \u0275dir=P({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Qe([Hk,jk]),Ze,Ye,tm(null)]})}return t})();var or=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var zk=(()=>{class t extends nr{callSetDisabledState;get submitted(){return fe(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Rt(()=>this._submittedReactive());_submittedReactive=ie(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(xu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){dE(e.control||null,e,!1),Vk(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,RE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Eu(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(dE(i||null,e),Uk(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);AE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&Ok(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){vg(this.form,this),this._oldForm&&xu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(D(Mu,10),D(hg,10),D(Au,8))};static \u0275dir=P({type:t,features:[Ze,Ye]})}return t})();var $k={provide:nr,useExisting:cn(()=>nc)},nc=(()=>{class t extends zk{form=null;ngSubmit=new X;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Wn(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&$("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Qe([$k]),Ze]})}return t})();var Gk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({})}return t})();var jt=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Au,useValue:e.callSetDisabledState??gg}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Gk]})}return t})();var Wk=new _("cdk-dir-doc",{providedIn:"root",factory:()=>u(V)}),qk=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function kE(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?qk.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var ti=(()=>{class t{get value(){return this.valueSignal()}valueSignal=ie("ltr");change=new X;constructor(){let e=u(Wk,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(kE(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Se=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({})}return t})();var Yk=["*"];var Zk=new _("MAT_CARD_CONFIG"),Bt=(()=>{class t{appearance;constructor(){let e=u(Zk,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&ae("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:Yk,decls:1,vars:0,template:function(i,r){i&1&&(Ke(),le(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})();var Ut=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Se]})}return t})();function ic(t){return t.buttons===0||t.detail===0}function rc(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var yg;function OE(){if(yg==null){let t=typeof document<"u"?document.head:null;yg=!!(t&&(t.createShadowRoot||t.attachShadow))}return yg}function _g(t){if(OE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function mt(t){if(t.composedPath)try{return t.composedPath()[0]}catch(n){}return t.target}var bg;try{bg=typeof Intl<"u"&&Intl.v8BreakIterator}catch(t){bg=!1}var Fe=(()=>{class t{_platformId=u(Lr);isBrowser=this._platformId?UC(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||bg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var oc;function FE(){if(oc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>oc=!0}))}finally{oc=oc||!1}return oc}function vs(t){return FE()?t:!!t.capture}function sn(t){return t instanceof H?t.nativeElement:t}var PE=new _("cdk-input-modality-detector-options"),LE={ignoreKeys:[18,17,224,91,16]},VE=650,Cg={passive:!0,capture:!0},jE=(()=>{class t{_platform=u(Fe);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new et(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=mt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<VE||(this._modality.next(ic(e)?"keyboard":"mouse"),this._mostRecentTarget=mt(e))};_onTouchstart=e=>{if(rc(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=mt(e)};constructor(){let e=u(k),i=u(V),r=u(PE,{optional:!0});if(this._options=C(C({},LE),r),this.modalityDetected=this._modality.pipe(Tf(1)),this.modalityChanged=this.modalityDetected.pipe(al()),this._platform.isBrowser){let o=u(qe).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Cg),o.listen(i,"mousedown",this._onMousedown,Cg),o.listen(i,"touchstart",this._onTouchstart,Cg)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),sc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(sc||{}),BE=new _("cdk-focus-monitor-default-options"),Ru=vs({passive:!0,capture:!0}),ac=(()=>{class t{_ngZone=u(k);_platform=u(Fe);_inputModalityDetector=u(jE);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(V);_stopInputModalityDetector=new I;constructor(){let e=u(BE,{optional:!0});this._detectionMode=e?.detectionMode||sc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=mt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=sn(e);if(!this._platform.isBrowser||r.nodeType!==1)return L();let o=_g(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new I,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=sn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=sn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===sc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===sc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?VE:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=mt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Ru),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Ru)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ne(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Ru),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Ru),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Nu=new WeakMap,kt=(()=>{class t{_appRef;_injector=u(ce);_environmentInjector=u(be);load(e){let i=this._appRef=this._appRef||this._injector.get(Jt),r=Nu.get(i);r||(r={loaders:new Set,refs:[]},Nu.set(i,r),i.onDestroy(()=>{Nu.get(i)?.refs.forEach(o=>o.destroy()),Nu.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Ad(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var cc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),ku;function Kk(){if(ku===void 0&&(ku=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(ku=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return ku}function eo(t){return Kk()?.createHTML(t)||t}function UE(t,n,e){let i=e.sanitize(nt.HTML,n);t.innerHTML=eo(i||"")}function Eg(t){return Array.isArray(t)?t:[t]}var HE=new Set,to,wg=(()=>{class t{_platform=u(Fe);_nonce=u(Vr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):Xk}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&Qk(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function Qk(t,n){if(!HE.has(t))try{to||(to=document.createElement("style"),n&&to.setAttribute("nonce",n),to.setAttribute("type","text/css"),document.head.appendChild(to)),to.sheet&&(to.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),HE.add(t))}catch(e){console.error(e)}}function Xk(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Jk=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var zE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({providers:[Jk]})}return t})();var $E=new _("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),GE=new _("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),eO=0,Dg=(()=>{class t{_ngZone=u(k);_defaultOptions=u(GE,{optional:!0});_liveElement;_document=u(V);_sanitizer=u(ka);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u($E,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:UE(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${eO++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var tO=200,Ou=class{_letterKeyStream=new I;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new I;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:tO;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(He(e=>this._pressedLetters.push(e)),Sf(n),Me(()=>this._pressedLetters.length>0),Q(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function ni(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var ys=class{_items;_activeItemIndex=ie(-1);_activeItem=ie(null);_wrap=!1;_typeaheadSubscription=de.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof vi?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):pn(n)&&(this._effectRef=Tn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new I;change=new I;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Ou(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||ni(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return pn(this._items)?this._items():this._items instanceof vi?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var lc=class extends ys{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var dc=class extends ys{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var ZE=new Map,wt=class t{_appId=u(Pr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=ZE.get(n);return i===void 0?i=0:i++,ZE.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})};var no;function KE(){if(no==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return no=!1,no;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)no=!0;else{let t=Element.prototype.scrollTo;t?no=!/\{\s*\[native code\]\s*\}/.test(t.toString()):no=!1}}return no}function Sg(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var _s,QE=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Mg(){if(_s)return _s;if(typeof document!="object"||!document)return _s=new Set(QE),_s;let t=document.createElement("input");return _s=new Set(QE.filter(n=>(t.setAttribute("type",n),t.type===n))),_s}var nO=new _("MATERIAL_ANIMATIONS"),XE=null;function Tg(){return u(nO,{optional:!0})?.animationsDisabled||u(ta,{optional:!0})==="NoopAnimations"?"di-disabled":(XE??=u(wg).matchMedia("(prefers-reduced-motion)").matches,XE?"reduced-motion":"enabled")}function Ht(){return Tg()!=="enabled"}function Ge(t){return t==null?"":typeof t=="string"?t:`${t}px`}function bs(t){return t!=null&&`${t}`!="false"}var wn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(wn||{}),Ag=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=wn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},ew=vs({passive:!0,capture:!0}),Rg=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,ew)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,ew)))}_delegateEventHandler=n=>{let e=mt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},uc={enterDuration:225,exitDuration:150},iO=800,tw=vs({passive:!0,capture:!0}),nw=["mousedown","touchstart"],iw=["mouseup","mouseleave","touchend","touchcancel"],rO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),fc=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Rg;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=sn(i)),o&&o.get(kt).load(rO)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},uc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||oO(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),h=f.transitionProperty,g=f.transitionDuration,b=h==="none"||g==="0s"||g==="0s, 0s"||r.width===0&&r.height===0,w=new Ag(this,d,i,b);d.style.transform="scale3d(1, 1, 1)",w.state=wn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=w);let R=null;return!b&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let J=()=>{R&&(R.fallbackTimer=null),clearTimeout(vt),this._finishRippleTransition(w)},Je=()=>this._destroyRipple(w),vt=setTimeout(Je,l+100);d.addEventListener("transitionend",J),d.addEventListener("transitioncancel",Je),R={onTransitionEnd:J,onTransitionCancel:Je,fallbackTimer:vt}}),this._activeRipples.set(w,R),(b||!l)&&this._finishRippleTransition(w),w}fadeOutRipple(n){if(n.state===wn.FADING_OUT||n.state===wn.HIDDEN)return;let e=n.element,i=C(C({},uc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=wn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=sn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,nw.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{iw.forEach(e=>{this._triggerElement.addEventListener(e,this,tw)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===wn.FADING_IN?this._startFadeOutTransition(n):n.state===wn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=wn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=wn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ic(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+iO;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!rc(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===wn.VISIBLE||n.config.terminateOnPointerUp&&n.state===wn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(nw.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(iw.forEach(e=>n.removeEventListener(e,this,tw)),this._pointerUpEventsRegistered=!1))}};function oO(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var hc=new _("mat-ripple-global-options"),rw=(()=>{class t{_elementRef=u(H);_animationsDisabled=Ht();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(k),i=u(Fe),r=u(hc,{optional:!0}),o=u(ce);this._globalOptions=r||{},this._rippleRenderer=new fc(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&ae("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var sO={capture:!0},aO=["focus","mousedown","mouseenter","touchstart"],Ng="mat-ripple-loader-uninitialized",kg="mat-ripple-loader-class-name",ow="mat-ripple-loader-centered",Fu="mat-ripple-loader-disabled",Pu=(()=>{class t{_document=u(V);_animationsDisabled=Ht();_globalRippleOptions=u(hc,{optional:!0});_platform=u(Fe);_ngZone=u(k);_injector=u(ce);_eventCleanups;_hosts=new Map;constructor(){let e=u(qe).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>aO.map(i=>e.listen(this._document,i,this._onInteraction,sO)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Ng,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(kg))&&e.setAttribute(kg,i.className||""),i.centered&&e.setAttribute(ow,""),i.disabled&&e.setAttribute(Fu,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Fu,""):e.removeAttribute(Fu)}_onInteraction=e=>{let i=mt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Ng}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(kg)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??uc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??uc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Fu),rippleConfig:{centered:e.hasAttribute(ow),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new fc(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(Ng)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var io=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2})}return t})();var cO=["*",[["","progressIndicator",""]]],lO=["*","[progressIndicator]"];function dO(t,n){t&1&&(Tt(0,"div",1),le(1,1),At())}var uO=new _("MAT_BUTTON_CONFIG");function sw(t){return t==null?void 0:yn(t)}var Og=(()=>{class t{_elementRef=u(H);_ngZone=u(k);_animationsDisabled=Ht();_config=u(uO,{optional:!0});_focusMonitor=u(ac);_cleanupClick;_renderer=u($e);_rippleLoader=u(Pu);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=Zo(!1,{transform:W});constructor(){u(kt).load(io);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(pe("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),On(r.color?"mat-"+r.color:""),ae("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",W],disabled:[2,"disabled","disabled",W],ariaDisabled:[2,"aria-disabled","ariaDisabled",W],disabledInteractive:[2,"disabledInteractive","disabledInteractive",W],tabIndex:[2,"tabIndex","tabIndex",sw],_tabindex:[2,"tabindex","_tabindex",sw],showProgress:[1,"showProgress"]}})}return t})(),ro=(()=>{class t extends Og{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Ze],ngContentSelectors:lO,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ke(cO),vn(0,"span",0),le(1),Ce(2,dO,2,0,"div",1),vn(3,"span",2)(4,"span",3)),i&2&&(v(2),Ee(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var Cs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Se]})}return t})();var fO=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],hO=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function pO(t,n){t&1&&(Tt(0,"div",2),le(1,3),At())}var aw=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Dn=(()=>{class t extends Og{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=mO(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?aw.get(this._appearance):null,o=aw.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Ze],ngContentSelectors:hO,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ke(fO),vn(0,"span",0),le(1),Tt(2,"span",1),le(3,1),At(),le(4,2),Ce(5,pO,2,0,"div",2),vn(6,"span",3)(7,"span",4)),i&2&&(ae("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),v(5),Ee(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --mat-progress-spinner-active-indicator-color: var(--mat-button-filled-progress-active-indicator-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function mO(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Dt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Cs,Se]})}return t})();var gO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),vO={passive:!0},cw=(()=>{class t{_platform=u(Fe);_ngZone=u(k);_renderer=u(qe).createRenderer(null,null);_styleLoader=u(kt);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return Ue;this._styleLoader.load(gO);let i=sn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new I,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,vO)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=sn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var lw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({})}return t})();var dw=new _("MAT_INPUT_VALUE_ACCESSOR");var Fg=class{_box;_destroyed=new I;_resizeSubject=new I;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ee(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Me(e=>e.some(i=>i.target===n)),dl({bufferSize:1,refCount:!0}),Ne(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},uw=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(k);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Fg(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var yO=["notch"],_O=["*"],fw=["iconPrefixContainer"],hw=["textPrefixContainer"],pw=["iconSuffixContainer"],mw=["textSuffixContainer"],bO=["textField"],CO=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],EO=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function wO(t,n){t&1&&ue(0,"span",21)}function DO(t,n){if(t&1&&(m(0,"label",20),le(1,1),Ce(2,wO,1,0,"span",21),p()),t&2){let e=S(2);x("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),pe("for",e._control.disableAutomaticLabeling?null:e._control.id),v(2),Ee(!e.hideRequiredMarker&&e._control.required?2:-1)}}function xO(t,n){if(t&1&&Ce(0,DO,3,5,"label",20),t&2){let e=S();Ee(e._hasFloatingLabel()?0:-1)}}function IO(t,n){t&1&&ue(0,"div",7)}function SO(t,n){}function MO(t,n){if(t&1&&re(0,SO,0,0,"ng-template",13),t&2){S(2);let e=Yi(1);x("ngTemplateOutlet",e)}}function TO(t,n){if(t&1&&(m(0,"div",9),Ce(1,MO,1,1,null,13),p()),t&2){let e=S();x("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),v(),Ee(e._forceDisplayInfixLabel()?-1:1)}}function AO(t,n){t&1&&(m(0,"div",10,2),le(2,2),p())}function RO(t,n){t&1&&(m(0,"div",11,3),le(2,3),p())}function NO(t,n){}function kO(t,n){if(t&1&&re(0,NO,0,0,"ng-template",13),t&2){S();let e=Yi(1);x("ngTemplateOutlet",e)}}function OO(t,n){t&1&&(m(0,"div",14,4),le(2,4),p())}function FO(t,n){t&1&&(m(0,"div",15,5),le(2,5),p())}function PO(t,n){t&1&&ue(0,"div",16)}function LO(t,n){t&1&&(m(0,"div",18),le(1,6),p())}function VO(t,n){if(t&1&&(m(0,"mat-hint",22),y(1),p()),t&2){let e=S(2);x("id",e._hintLabelId),v(),it(e.hintLabel)}}function jO(t,n){if(t&1&&(m(0,"div",19),Ce(1,VO,2,2,"mat-hint",22),le(2,7),ue(3,"div",23),le(4,8),p()),t&2){let e=S();v(),Ee(e.hintLabel?1:-1)}}var gt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-label"]]})}return t})(),BO=new _("MatError");var pc=(()=>{class t{align="start";id=u(wt).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Zn("id",r.id),pe("align",null),ae("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),Ew=new _("MatPrefix"),oo=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matPrefix",""],["","matIconPrefix",""],["","matTextPrefix",""]],inputs:{_isTextSelector:[0,"matTextPrefix","_isTextSelector"]},features:[Qe([{provide:Ew,useExisting:t}])]})}return t})(),ww=new _("MatSuffix"),an=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[Qe([{provide:ww,useExisting:t}])]})}return t})(),Dw=new _("FloatingLabelParent"),gw=(()=>{class t{_elementRef=u(H);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(uw);_ngZone=u(k);_parent=u(Dw);_resizeSubscription=new de;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return UO(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&ae("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function UO(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var vw="mdc-line-ripple--active",Lu="mdc-line-ripple--deactivating",yw=(()=>{class t{_elementRef=u(H);_cleanupTransitionEnd;constructor(){let e=u(k),i=u($e);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Lu),e.add(vw)}deactivate(){this._elementRef.nativeElement.classList.add(Lu)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Lu);e.propertyName==="opacity"&&r&&i.remove(vw,Lu)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),_w=(()=>{class t{_elementRef=u(H);_ngZone=u(k);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Lt(yO,5),i&2){let o;oe(o=se())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&ae("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:_O,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Ke(),vn(0,"div",1),Tt(1,"div",2,0),le(3),At(),vn(4,"div",3))},encapsulation:2})}return t})(),mc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t})}return t})();var gc=new _("MatFormField"),HO=new _("MAT_FORM_FIELD_DEFAULT_OPTIONS"),bw="fill",zO="auto",Cw="fixed",$O="translateY(-50%)",xt=(()=>{class t{_elementRef=u(H);_changeDetectorRef=u(me);_platform=u(Fe);_idGenerator=u(wt);_ngZone=u(k);_defaults=u(HO,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=wa("iconPrefixContainer");_textPrefixContainerSignal=wa("textPrefixContainer");_iconSuffixContainerSignal=wa("iconSuffixContainer");_textSuffixContainerSignal=wa("textSuffixContainer");_prefixSuffixContainers=Rt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=AC(gt);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=bs(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||zO}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||bw;this._appearanceSignal.set(i)}_appearanceSignal=ie(bw);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||Cw}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||Cw}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new I;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ht();constructor(){let e=this._defaults,i=u(ti);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Tn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Rt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Zt([void 0,void 0]),Q(()=>[i.errorState,i.userAriaDescribedBy]),ll(),Me(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ne(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),jn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){kC({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Rt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,g=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,b=`var(--mat-mdc-form-field-label-transform, ${$O} translateX(${g}))`,w=s+a+c+l;return[b,w]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(wd(o,r._labelChild,gt,5),Kn(o,mc,5)(o,Ew,5)(o,ww,5)(o,BO,5)(o,pc,5)),i&2){xd();let s;oe(s=se())&&(r._formFieldControl=s.first),oe(s=se())&&(r._prefixChildren=s),oe(s=se())&&(r._suffixChildren=s),oe(s=se())&&(r._errorChildren=s),oe(s=se())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Dd(r._iconPrefixContainerSignal,fw,5)(r._textPrefixContainerSignal,hw,5)(r._iconSuffixContainerSignal,pw,5)(r._textSuffixContainerSignal,mw,5),Lt(bO,5)(fw,5)(hw,5)(pw,5)(mw,5)(gw,5)(_w,5)(yw,5)),i&2){xd(4);let o;oe(o=se())&&(r._textField=o.first),oe(o=se())&&(r._iconPrefixContainer=o.first),oe(o=se())&&(r._textPrefixContainer=o.first),oe(o=se())&&(r._iconSuffixContainer=o.first),oe(o=se())&&(r._textSuffixContainer=o.first),oe(o=se())&&(r._floatingLabel=o.first),oe(o=se())&&(r._notchedOutline=o.first),oe(o=se())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&ae("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Qe([{provide:gc,useExisting:t},{provide:Dw,useExisting:t}])],ngContentSelectors:EO,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Ke(CO),re(0,xO,1,1,"ng-template",null,0,Ca),m(2,"div",6,1),$("click",function(s){return r._control.onContainerClick(s)}),Ce(4,IO,1,0,"div",7),m(5,"div",8),Ce(6,TO,2,2,"div",9),Ce(7,AO,3,0,"div",10),Ce(8,RO,3,0,"div",11),m(9,"div",12),Ce(10,kO,1,1,null,13),le(11),p(),Ce(12,OO,3,0,"div",14),Ce(13,FO,3,0,"div",15),p(),Ce(14,PO,1,0,"div",16),p(),m(15,"div",17),Ce(16,LO,2,0,"div",18)(17,jO,5,1,"div",19),p()),i&2){let o;v(2),ae("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),v(2),Ee(!r._hasOutline()&&!r._control.disabled?4:-1),v(2),Ee(r._hasOutline()?6:-1),v(),Ee(r._hasIconPrefix?7:-1),v(),Ee(r._hasTextPrefix?8:-1),v(2),Ee(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),v(2),Ee(r._hasTextSuffix?12:-1),v(),Ee(r._hasIconSuffix?13:-1),v(),Ee(r._hasOutline()?-1:14),v(),ae("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();v(),Ee((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[gw,_w,xa,yw,pc],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var Es=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var ws=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var rt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[zE,xt,Se]})}return t})();var GO=["button","checkbox","file","hidden","image","radio","range","reset","submit"],WO=new _("MAT_INPUT_CONFIG"),zt=(()=>{class t{_elementRef=u(H);_platform=u(Fe);ngControl=u(Jn,{optional:!0,self:!0});_autofillMonitor=u(cw);_ngZone=u(k);_formField=u(gc,{optional:!0});_renderer=u($e);_uid=u(wt).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(WO,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new I;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=bs(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Qr.required)??!1}set required(e){this._required=bs(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Mg().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=bs(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Mg().has(e));constructor(){let e=u(on,{optional:!0}),i=u(nc,{optional:!0}),r=u(Es),o=u(dw,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?pn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new ws(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Tn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){GO.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&$("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Zn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),pe("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),ae("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",W]},exportAs:["matInput"],features:[Qe([{provide:mc,useExisting:t}]),Ye]})}return t})(),$t=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[rt,rt,lw,Se]})}return t})();function xw(t){return Error(`Unable to find icon with the name "${t}"`)}function qO(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function Iw(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function Sw(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Mi=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},Tw=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Mi(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(nt.HTML,r);if(!s)throw Sw(r);let a=eo(s);return this._addSvgIconConfig(e,i,new Mi("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Mi(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(nt.HTML,i);if(!o)throw Sw(i);let s=eo(o);return this._addSvgIconSetConfig(e,new Mi("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(nt.RESOURCE_URL,e);if(!i)throw Iw(e);let r=this._cachedIconsByUrl.get(i);return r?L(Vu(r)):this._loadSvgIconFromConfig(new Mi(e,null)).pipe(He(o=>this._cachedIconsByUrl.set(i,o)),Q(o=>Vu(o)))}getNamedSvgIcon(e,i=""){let r=Mw(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Ls(xw(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?L(Vu(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(Q(i=>Vu(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return L(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Cr(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(nt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),L(null)})));return js(o).pipe(Q(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw xw(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(He(i=>e.svgText=i),Q(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?L(null):this._fetchIcon(e).pipe(He(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(eo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(eo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw qO();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(nt.RESOURCE_URL,i);if(!s)throw Iw(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(Q(l=>eo(l)),Er(()=>this._inProgressUrlFetches.delete(s)),Bs());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(Mw(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return YO(o)?new Mi(o.url,null,o.options):new Mi(o,null)}}static \u0275fac=function(i){return new(i||t)(M(Cn,8),M(ka),M(V,8),M(Mt))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Vu(t){return t.cloneNode(!0)}function Mw(t,n){return t+":"+n}function YO(t){return!!(t.url&&t.options)}var ZO=["*"],KO=new _("MAT_ICON_DEFAULT_OPTIONS"),QO=new _("mat-icon-location",{providedIn:"root",factory:()=>{let t=u(V),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),Aw=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],XO=Aw.map(t=>`[${t}]`).join(", "),JO=/^url\(['"]?#(.*?)['"]?\)$/,Gt=(()=>{class t{_elementRef=u(H);_iconRegistry=u(Tw);_location=u(QO);_errorHandler=u(Mt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=de.EMPTY;constructor(){let e=u(new Zi("aria-hidden"),{optional:!0}),i=u(KO,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(XO),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)Aw.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(JO):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Yt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(pe("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),On(r.color?"mat-"+r.color:""),ae("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",W],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:ZO,decls:1,vars:0,template:function(i,r){i&1&&(Ke(),le(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})(),Wt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Se]})}return t})();var je={production:!1,apiUrl:"http://localhost:8080/tela-login-angular"};var xn=class t{constructor(n){this.http=n}http;login(n,e){return this.http.post(`${je.apiUrl}/auth/login`,{email:n,senha:e}).pipe(He(i=>{localStorage.setItem("token",i.token),localStorage.setItem("id",i.id),localStorage.setItem("nome",i.nome),localStorage.setItem("email",i.email),localStorage.setItem("role",i.role)}))}logout(){let n=localStorage.getItem("token");localStorage.clear(),n&&this.http.post(`${je.apiUrl}/auth/logout`,{},{headers:{Authorization:`Bearer ${n}`}}).subscribe()}getToken(){return localStorage.getItem("token")}isLoggedIn(){return!!this.getToken()}novoUsuario(n,e,i){return this.http.post(`${je.apiUrl}/usuario`,{nome:n,email:e,senha:i})}static \u0275fac=function(e){return new(e||t)(M(Cn))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};function eF(t,n){if(t&1&&(m(0,"div",14),y(1),p()),t&2){let e=S();v(),Te(" ",e.mensagens," ")}}function tF(t,n){t&1&&(m(0,"mat-icon"),y(1," login "),p())}var ju=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;email="";senha="";mensagens="";carregando=!1;onSubmit(){this.carregando=!0,this.mensagens="",this.authService.login(this.email,this.senha).subscribe({next:n=>{console.log("Login realizado com sucesso",n),this.carregando=!1,this.cdr.detectChanges(),this.router.navigate(["/pets"])},error:n=>{console.error("Falha no login",n),this.carregando=!1,n.status===401?this.mensagens=n.error?.erro??"Email ou senha invalidos.":n.status===0?this.mensagens=n.error?.erro??"Nao foi possivel conectar ao servidor.":this.mensagens=n.error?.erro??"Erro inesperado. Tente novamente.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(xn),D(Xe),D(me))};static \u0275cmp=O({type:t,selectors:[["app-login"]],decls:30,vars:6,consts:[[1,"login-container"],[1,"login-card"],[1,"login-header"],[1,"login-icon"],[3,"ngSubmit"],["appearance","outline"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["class","error",4,"ngIf"],["mat-raised-button","","color","primary","type","submit",1,"full-width",3,"disabled"],[4,"ngIf"],[1,"switch-link"],["routerLink","/criar-usuario"],[1,"error"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"mat-icon",3),y(4," pets "),p(),m(5,"h2"),y(6,"Entrar"),p(),m(7,"p"),y(8,"Bem-vindo ao PetManager"),p()(),m(9,"form",4),$("ngSubmit",function(){return i.onSubmit()}),m(10,"mat-form-field",5)(11,"mat-label"),y(12,"Email"),p(),m(13,"input",6),Oe("ngModelChange",function(o){return Ve(i.email,o)||(i.email=o),o}),p(),Pe(),m(14,"mat-icon",7),y(15," email "),p()(),m(16,"mat-form-field",5)(17,"mat-label"),y(18,"Senha"),p(),m(19,"input",8),Oe("ngModelChange",function(o){return Ve(i.senha,o)||(i.senha=o),o}),p(),Pe(),m(20,"mat-icon",7),y(21," lock "),p()(),re(22,eF,2,1,"div",9),m(23,"button",10),re(24,tF,2,0,"mat-icon",11),y(25),p()(),m(26,"div",12),y(27," N\xE3o possui uma conta? "),m(28,"a",13),y(29," Criar conta "),p()()()()),e&2&&(v(13),ke("ngModel",i.email),Le(),v(6),ke("ngModel",i.senha),Le(),v(3),x("ngIf",i.mensagens),v(),x("disabled",i.carregando),v(),x("ngIf",!i.carregando),v(),Te(" ",i.carregando?"Entrando...":"Entrar"," "))},dependencies:[jt,or,Et,Vt,rr,ei,Nt,on,ct,pt,Ct,Ut,Bt,Dt,Dn,rt,xt,gt,an,$t,zt,Wt,Gt],encapsulation:2})};function nF(t,n){if(t&1&&(m(0,"p"),y(1),p()),t&2){let e=S();v(),Te(" ",e.mensagens," ")}}var Bu=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;nome="";email="";senha="";mensagens="";onSubmit(){this.authService.novoUsuario(this.nome,this.email,this.senha).subscribe({next:n=>{console.log("Usuario criado com sucesso",n),this.mensagens="Usuario criado com sucesso.",this.router.navigate(["/login"]),this.cdr.detectChanges()},error:n=>{console.error("Nao foi possivel criar este usuario",n),this.mensagens=n.error?.erro??"Nao foi possivel criar este usuario. Verifique se as informa\xE7\xF5es s\xE3o v\xE1lidas",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(xn),D(Xe),D(me))};static \u0275cmp=O({type:t,selectors:[["app-criar-usuario"]],decls:24,vars:4,consts:[[1,"login-container"],[1,"login-card"],[3,"ngSubmit"],["appearance","outline"],["matInput","","name","nome","required","",3,"ngModelChange","ngModel"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["mat-raised-button","","color","primary","type","submit",1,"full-width"],[4,"ngIf"],[1,"switch-link"],["routerLink","/login"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card",1)(2,"h2"),y(3,"Criar conta"),p(),m(4,"form",2),$("ngSubmit",function(){return i.onSubmit()}),m(5,"mat-form-field",3)(6,"mat-label"),y(7,"Nome"),p(),m(8,"input",4),Oe("ngModelChange",function(o){return Ve(i.nome,o)||(i.nome=o),o}),p(),Pe(),p(),m(9,"mat-form-field",3)(10,"mat-label"),y(11,"Email"),p(),m(12,"input",5),Oe("ngModelChange",function(o){return Ve(i.email,o)||(i.email=o),o}),p(),Pe(),p(),m(13,"mat-form-field",3)(14,"mat-label"),y(15,"Senha (m\xEDnimo de 4 caracteres)"),p(),m(16,"input",6),Oe("ngModelChange",function(o){return Ve(i.senha,o)||(i.senha=o),o}),p(),Pe(),p(),m(17,"button",7),y(18," Criar conta "),p()(),re(19,nF,2,1,"p",8),m(20,"div",9),y(21," J\xE1 possui uma conta? "),m(22,"a",10),y(23," Entrar "),p()()()()),e&2&&(v(8),ke("ngModel",i.nome),Le(),v(4),ke("ngModel",i.email),Le(),v(4),ke("ngModel",i.senha),Le(),v(3),x("ngIf",i.mensagens))},dependencies:[jt,or,Et,Vt,rr,ei,Nt,on,ct,pt,Ct,Ut,Bt,rt,xt,gt,$t,zt,Dt,Dn],styles:[".full-width[_ngcontent-%COMP%]{width:100%}.switch-link[_ngcontent-%COMP%]{margin-top:20px;text-align:center}.switch-link[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{font-weight:600}"]})};var iF=["determinateSpinner"];function rF(t,n){if(t&1&&(zi(),m(0,"svg",11),ue(1,"circle",12),p()),t&2){let e=S();pe("viewBox",e._viewBox()),v(),$r("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),pe("r",e._circleRadius())}}var oF=new _("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:Rw})}),Rw=100,sF=10,ii=(()=>{class t{_elementRef=u(H);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=u(oF),i=Tg(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=Rw;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-sF)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&Lt(iF,5),i&2){let o;oe(o=se())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(pe("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),On("mat-"+r.color),$r("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),ae("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",yn],diameter:[2,"diameter","diameter",yn],strokeWidth:[2,"strokeWidth","strokeWidth",yn]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(re(0,rF,2,8,"ng-template",null,0,Ca),m(2,"div",2,1),zi(),m(4,"svg",3),ue(5,"circle",4),p()(),Pl(),m(6,"div",5)(7,"div",6)(8,"div",7),Yo(9,8),p(),m(10,"div",9),Yo(11,8),p(),m(12,"div",10),Yo(13,8),p()()()),i&2){let o=Yi(1);v(4),pe("viewBox",r._viewBox()),v(),$r("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),pe("r",r._circleRadius()),v(4),x("ngTemplateOutlet",o),v(2),x("ngTemplateOutlet",o),v(2),x("ngTemplateOutlet",o)}},dependencies:[xa],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2})}return t})();var ri=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Se]})}return t})();var aF=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],cF=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function lF(t,n){t&1&&(m(0,"span",3),le(1,1),p())}function dF(t,n){t&1&&(m(0,"span",6),le(1,2),p())}var uF=["*"];var fF=new _("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),Nw=new _("MatChipAvatar"),kw=new _("MatChipTrailingIcon"),Ow=new _("MatChipEdit"),Fw=new _("MatChipRemove"),Pw=new _("MatChip"),Lw=(()=>{class t{_elementRef=u(H);_parentChip=u(Pw);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){u(kt).load(io),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(pe("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),ae("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",W],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:yn(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return t})(),hF=(()=>{class t extends Lw{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Wn(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&$("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(pe("tabindex",r._getTabindex()),ae("mdc-evolution-chip__action--presentational",!1))},features:[Ze]})}return t})();var Pg=(()=>{class t{_changeDetectorRef=u(me);_elementRef=u(H);_tagName=u(TC);_ngZone=u(k);_focusMonitor=u(ac);_globalRippleOptions=u(hc,{optional:!0});_document=u(V);_onFocus=new I;_onBlur=new I;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Ht();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=u(wt).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new X;destroyed=new X;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=u(Pu);_injector=u(ce);constructor(){let e=u(kt);e.load(io),e.load(cc),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=jn(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&Kn(o,Nw,5)(o,Ow,5)(o,kw,5)(o,Fw,5)(o,Nw,5)(o,kw,5)(o,Ow,5)(o,Fw,5),i&2){let s;oe(s=se())&&(r.leadingIcon=s.first),oe(s=se())&&(r.editIcon=s.first),oe(s=se())&&(r.trailingIcon=s.first),oe(s=se())&&(r.removeIcon=s.first),oe(s=se())&&(r._allLeadingIcons=s),oe(s=se())&&(r._allTrailingIcons=s),oe(s=se())&&(r._allEditIcons=s),oe(s=se())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&Lt(hF,5),i&2){let o;oe(o=se())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&$("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Zn("id",r.id),pe("role",r.role)("aria-label",r.ariaLabel),On("mat-"+(r.color||"primary")),ae("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",W],highlighted:[2,"highlighted","highlighted",W],disableRipple:[2,"disableRipple","disableRipple",W],disabled:[2,"disabled","disabled",W]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[Qe([{provide:Pw,useExisting:t}])],ngContentSelectors:cF,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(Ke(aF),ue(0,"span",0),m(1,"span",1)(2,"span",2),Ce(3,lF,2,0,"span",3),m(4,"span",4),le(5),ue(6,"span",5),p()()(),Ce(7,dF,2,0,"span",6)),i&2&&(v(3),Ee(r.leadingIcon?3:-1),v(4),Ee(r._hasTrailingIcon()?7:-1))},dependencies:[Lw],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return t})();var Vw=(()=>{class t{_elementRef=u(H);_changeDetectorRef=u(me);_dir=u(ti,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new I;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(e=>e._onFocus)}get chipDestroyedChanges(){return this._getChipStream(e=>e.destroyed)}get chipRemovedChanges(){return this._getChipStream(e=>e.removed)}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(e){this._explicitRole=e}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new vi;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(e=>e._hasFocus())}_syncChipsState(){this._chips?.forEach(e=>{e._chipListDisabled=this._disabled,e._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(e){this._originatesFromChip(e)&&this._keyManager.onKeydown(e)}_isValidIndex(e){return e>=0&&e<this._chips.length}_allowFocusEscape(){let e=this._elementRef.nativeElement.tabIndex;e!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=e))}_getChipStream(e){return this._chips.changes.pipe(Zt(null),tt(()=>jn(...this._chips.map(e))))}_originatesFromChip(e){let i=e.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(Zt(this._chips)).subscribe(e=>{let i=[];e.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new dc(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(e=>this._skipPredicate(e)),this.chipFocusChanges.pipe(Ne(this._destroyed)).subscribe(({chip:e})=>{let i=e._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(Ne(this._destroyed)).subscribe(e=>this._keyManager.withHorizontalOrientation(e))}_skipPredicate(e){return e.disabled}_trackChipSetChanges(){this._chips.changes.pipe(Zt(null),Ne(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(Ne(this._destroyed)).subscribe(e=>{let r=this._chips.toArray().indexOf(e.chip),o=e.chip._hasFocus(),s=e.chip._hadFocusOnRemove&&this._keyManager.activeItem&&e.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let e=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[e];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&Kn(o,Pg,5),i&2){let s;oe(s=se())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&$("keydown",function(s){return r._handleKeydown(s)}),i&2&&pe("role",r.role)},inputs:{disabled:[2,"disabled","disabled",W],role:"role",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:yn(e)]},ngContentSelectors:uF,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(Ke(),Tt(0,"div",0),le(1),At())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})}return t})();var Uu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({providers:[Es,{provide:fF,useValue:{separatorKeyCodes:[13]}}],imports:[Cs,Se]})}return t})();var Ds=class t{constructor(n){this.http=n}http;todos(){return this.http.get(`${je.apiUrl}/usuario/all`)}porId(n){return this.http.get(`${je.apiUrl}/usuario/${n}`)}porEmail(n){return this.http.get(`${je.apiUrl}/usuario`,{params:{email:n}})}atualizar(n){return this.http.put(`${je.apiUrl}/usuario`,n)}remover(n){return this.http.delete(`${je.apiUrl}/usuario/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Cn))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};var mF=t=>["/usuarios",t];function gF(t,n){if(t&1){let e=at();m(0,"button",10),$("click",function(){xe(e);let r=S();return r.termoBusca="",Ie(r.filtrarUsuarios())}),m(1,"mat-icon"),y(2,"close"),p()()}}function vF(t,n){if(t&1&&(m(0,"div",11),y(1),p()),t&2){let e=S();v(),Te(" ",e.mensagens," ")}}function yF(t,n){t&1&&(m(0,"div",12),ue(1,"mat-spinner",13),p())}function _F(t,n){if(t&1){let e=at();m(0,"tr")(1,"td"),y(2),p(),m(3,"td"),y(4),p(),m(5,"td")(6,"mat-chip-set")(7,"mat-chip",17),y(8),p()()(),m(9,"td",18)(10,"button",19)(11,"mat-icon"),y(12,"edit"),p()(),m(13,"button",20),$("click",function(){let r=xe(e).$implicit,o=S(2);return Ie(o.remover(r.id))}),m(14,"mat-icon"),y(15,"delete"),p()()()()}if(t&2){let e=n.$implicit;v(2),it(e.nome),v(2),it(e.email),v(3),x("color",e.role==="ADMIN"?"primary":"accent"),v(),Te(" ",e.role," "),v(2),x("routerLink",ba(5,mF,e.id))}}function bF(t,n){if(t&1&&(m(0,"table",14)(1,"thead")(2,"tr")(3,"th"),y(4,"Nome"),p(),m(5,"th"),y(6,"Email"),p(),m(7,"th"),y(8,"Perfil"),p(),m(9,"th",15),y(10,"A\xE7\xF5es"),p()()(),m(11,"tbody"),re(12,_F,16,7,"tr",16),p()()),t&2){let e=S();v(12),x("ngForOf",e.usuariosFiltrados)}}function CF(t,n){t&1&&(m(0,"p"),y(1,"N\xE3o h\xE1 usu\xE1rios cadastrados."),p())}function EF(t,n){t&1&&(m(0,"p"),y(1,"Tente outro termo de busca."),p())}function wF(t,n){if(t&1&&(m(0,"div",21)(1,"mat-icon",22),y(2," group "),p(),m(3,"h3"),y(4),p(),re(5,CF,2,0,"p",23)(6,EF,2,0,"p",23),p()),t&2){let e=S();v(4),it((e.termoBusca,"Nenhum usu\xE1rio encontrado")),v(),x("ngIf",!e.termoBusca),v(),x("ngIf",e.termoBusca)}}var Hu=class t{constructor(n,e,i,r){this.usuarioService=n;this.authService=e;this.cdr=i;this.router=r}usuarioService;authService;cdr;router;usuarios=[];carregando=!0;mensagens="";usuariosFiltrados=[];termoBusca="";ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.usuarioService.todos().subscribe({next:n=>{this.usuarios=n,this.filtrarUsuarios(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar usu\xE1rios.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarUsuarios(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.usuariosFiltrados=this.usuarios;return}this.usuariosFiltrados=this.usuarios.filter(e=>e.nome?.toLowerCase().includes(n)||e.email?.toLowerCase().includes(n)||e.role?.toLowerCase().includes(n))}remover(n){if(n==null||!confirm("Confirma\xE7ao para remover este usuario"))return;let e=Number(localStorage.getItem("id"))===n;this.usuarioService.remover(n).subscribe({next:()=>{if(e){this.authService.logout(),this.router.navigate(["/login"]),this.cdr.detectChanges();return}this.usuarios=this.usuarios.filter(i=>i.id!==n),this.filtrarUsuarios(),this.cdr.detectChanges()},error:i=>{console.error("Erro ao remover usuario",i),this.mensagens="Nao foi possivel remover este usuario.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(Ds),D(xn),D(me),D(Xe))};static \u0275cmp=O({type:t,selectors:[["app-gestao-usuarios"]],decls:16,vars:6,consts:[[1,"page"],[1,"header"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, email ou perfil...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],["selected","",3,"color"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),y(4,"\u{1F465} Gest\xE3o de Usu\xE1rios"),p()(),m(5,"mat-form-field",2)(6,"mat-label"),y(7,"Pesquisar"),p(),m(8,"input",3),Oe("ngModelChange",function(o){return Ve(i.termoBusca,o)||(i.termoBusca=o),o}),$("ngModelChange",function(){return i.filtrarUsuarios()}),p(),Pe(),m(9,"mat-icon",4),y(10,"search"),p(),re(11,gF,3,0,"button",5),p(),re(12,vF,2,1,"div",6)(13,yF,2,0,"div",7)(14,bF,13,1,"table",8)(15,wF,7,3,"div",9),p()()),e&2&&(v(8),ke("ngModel",i.termoBusca),Le(),v(3),x("ngIf",i.termoBusca),v(),x("ngIf",i.mensagens),v(),x("ngIf",i.carregando),v(),x("ngIf",!i.carregando&&i.usuariosFiltrados.length>0),v(),x("ngIf",!i.carregando&&i.usuariosFiltrados.length===0))},dependencies:[ct,bn,pt,jt,Et,Vt,Nt,Ct,Ut,Bt,Dt,ro,Wt,Gt,ri,ii,rt,xt,gt,oo,an,$t,zt,Uu,Pg,Vw],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};var bc=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new I;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var DF=20,Lg=(()=>{class t{_ngZone=u(k);_platform=u(Fe);_renderer=u(qe).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new I;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=DF){return this._platform.isBrowser?new ee(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(sl(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):L()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Me(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=sn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var xF=20,uo=(()=>{class t{_platform=u(Fe);_listeners;_viewportSize=null;_change=new I;_document=u(V);constructor(){let e=u(k),i=u(qe).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=xF){return e>0?this._change.pipe(sl(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var zu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({})}return t})(),Vg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Se,zu,Se,zu]})}return t})();var Cc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},jg=class extends Cc{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Ec=class extends Cc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Bg=class extends Cc{element;constructor(n){super(),this.element=n instanceof H?n.nativeElement:n}},Ug=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof jg)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Ec)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Bg)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},$u=class extends Ug{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(_i,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||ce.NULL,o=r.get(be,i.injector);e=Ad(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Bw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({})}return t})();var Uw=KE();function Zw(t){return new Gu(t.get(uo),t.get(V))}var Gu=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Ge(-this._previousScrollPosition.left),n.style.top=Ge(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),Uw&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),Uw&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function Kw(t,n){return new Wu(t.get(Lg),t.get(k),t.get(uo),n)}var Wu=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Me(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var wc=class{enable(){}disable(){}attach(){}};function Hg(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function Hw(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function xc(t,n){return new qu(t.get(Lg),t.get(uo),t.get(k),n)}var qu=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Hg(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},Qw=(()=>{class t{_injector=u(ce);noop=()=>new wc;close=e=>Kw(this._injector,e);block=()=>Zw(this._injector);reposition=e=>xc(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Dc=class{positionStrategy;scrollStrategy=new wc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Yu=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var Xw=(()=>{class t{_attachedOverlays=[];_document=u(V);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Jw=(()=>{class t extends Xw{_ngZone=u(k);_renderer=u(qe).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),eD=(()=>{class t extends Xw{_platform=u(Fe);_ngZone=u(k);_renderer=u(qe).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=mt(e)};_clickListener=e=>{let i=mt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(zw(a.overlayElement,i)||zw(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function zw(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var tD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),nD=(()=>{class t{_platform=u(Fe);_containerElement;_document=u(V);_styleLoader=u(kt);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Sg()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Sg()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(tD)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),zg=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function $g(t){return t&&t.nodeType===1}var Zu=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new I;_attachments=new I;_detachments=new I;_positionStrategy;_scrollStrategy;_locationChanges=de.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new I;_outsidePointerEvents=new I;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,d=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Yn(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=j(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Ge(this._config.width),n.height=Ge(this._config.height),n.minWidth=Ge(this._config.minWidth),n.minHeight=Ge(this._config.minHeight),n.maxWidth=Ge(this._config.maxWidth),n.maxHeight=Ge(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;$g(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch(n){}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new zg(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Eg(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Yn(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},$w="cdk-overlay-connected-position-bounding-box",IF=/([A-Za-z%]+)$/;function Gg(t,n){return new Ku(n,t.get(uo),t.get(V),t.get(Fe),t.get(nD))}var Ku=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new I;_resizeSubscription=de.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add($w),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),d=this._getOverlayFit(l,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&fo(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove($w),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof H?this._origin.nativeElement:$g(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=Ww(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,f=s+o.width-i.width,h=0-a,g=a+o.height-i.height,b=this._subtractOverflows(o.width,d,f),w=this._subtractOverflows(o.height,h,g),R=b*w;return{visibleArea:R,isCompletelyWithinViewport:o.width*o.height===R,fitsInViewportVertically:w===o.height,fitsInViewportHorizontally:b==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=Gw(this._overlayRef.getConfig().minHeight),a=Gw(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=Ww(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!SF(this._lastScrollVisibility,i)){let r=new Yu(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let g=Math.min(i.bottom-n.y+i.top,n.y),b=this._lastBoundingBoxSize.height;o=g*2,s=n.y-g,o>b&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-b/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,f,h;if(l)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let g=Math.min(i.right-n.x+i.left,n.x),b=this._lastBoundingBoxSize.width;d=g*2,f=n.x-g,d>b&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-b/2)}return{top:s,left:f,bottom:a,right:h,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Ge(i.width),r.height=Ge(i.height),r.top=Ge(i.top)||"auto",r.bottom=Ge(i.bottom)||"auto",r.left=Ge(i.left)||"auto",r.right=Ge(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Ge(o)),s&&(r.maxWidth=Ge(s))}this._lastBoundingBoxSize=i,fo(this._boundingBox.style,r)}_resetBoundingBoxStyles(){fo(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){fo(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();fo(i,this._getExactOverlayY(e,n,d)),fo(i,this._getExactOverlayX(e,n,d))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Ge(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Ge(s.maxWidth):o&&(i.maxWidth="")),fo(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Ge(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Ge(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:Hw(n,i),isOriginOutsideView:Hg(n,i),isOverlayClipped:Hw(e,i),isOverlayOutsideView:Hg(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Eg(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof H)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function fo(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function Gw(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(IF);return!e||e==="px"?parseFloat(n):null}return t||null}function Ww(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function SF(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var qw="cdk-global-overlay-wrapper";function iD(t){return new Qu}var Qu=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(qw),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",g="",b="",w="";c?w="flex-start":d==="center"?(w="center",h?b=f:g=f):h?d==="left"||d==="end"?(w="flex-end",g=f):(d==="right"||d==="start")&&(w="flex-start",b=f):d==="left"||d==="start"?(w="flex-start",g=f):(d==="right"||d==="end")&&(w="flex-end",b=f),n.position=this._cssPosition,n.marginLeft=c?"0":g,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":b,e.justifyContent=w,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(qw),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},rD=(()=>{class t{_injector=u(ce);global(){return iD()}flexibleConnectedTo(e){return Gg(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Ic=new _("OVERLAY_DEFAULT_CONFIG");function Wg(t,n){t.get(kt).load(tD);let e=t.get(nD),i=t.get(V),r=t.get(wt),o=t.get(Jt),s=t.get(ti),a=t.get($e,null,{optional:!0})||t.get(qe).createRenderer(null,null),c=new Dc(n),l=t.get(Ic,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return $g(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new Zu(new $u(d,o,t),f,d,c,t.get(k),t.get(Jw),i,t.get(Xi),t.get(eD),n?.disableAnimations??t.get(ta,null,{optional:!0})==="NoopAnimations",t.get(be),a)}var oD=(()=>{class t{scrollStrategies=u(Qw);_positionBuilder=u(rD);_injector=u(ce);create(e){return Wg(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),MF=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],TF=new _("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ce);return()=>xc(t)}}),xs=(()=>{class t{elementRef=u(H);static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),sD=new _("cdk-connected-overlay-default-config"),Xu=(()=>{class t{_dir=u(ti,{optional:!0});_injector=u(ce);_overlayRef;_templatePortal;_backdropSubscription=de.EMPTY;_attachSubscription=de.EMPTY;_detachSubscription=de.EMPTY;_positionSubscription=de.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(TF);_ngZone=u(k);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new X;positionChange=new X;attach=new X;detach=new X;overlayKeydown=new X;overlayOutsideClick=new X;constructor(){let e=u(mn),i=u(Pt),r=u(sD,{optional:!0}),o=u(Ic,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Ec(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=MF);let e=this._overlayRef=Wg(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!ni(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=mt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Dc({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Gg(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof xs?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof xs?this.origin.elementRef.nativeElement:this.origin instanceof H?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Af(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",W],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",W],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",W],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",W],push:[2,"cdkConnectedOverlayPush","push",W],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",W],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",W],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Ye]})}return t})(),qg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({providers:[oD],imports:[Se,Bw,Vg,Vg]})}return t})();var aD=(()=>{class t{_animationsDisabled=Ht();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&ae("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var AF=["text"],RF=[[["mat-icon"]],"*"],NF=["mat-icon","*"];function kF(t,n){if(t&1&&ue(0,"mat-pseudo-checkbox",1),t&2){let e=S();x("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function OF(t,n){if(t&1&&ue(0,"mat-pseudo-checkbox",3),t&2){let e=S();x("disabled",e.disabled)}}function FF(t,n){if(t&1&&(m(0,"span",4),y(1),p()),t&2){let e=S();v(),Te("(",e.group.label,")")}}var Zg=new _("MAT_OPTION_PARENT_COMPONENT"),Kg=new _("MatOptgroup");var Yg=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},ar=(()=>{class t{_element=u(H);_changeDetectorRef=u(me);_parent=u(Zg,{optional:!0});group=u(Kg,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(wt).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ie(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new X;_text;_stateChanges=new I;constructor(){let e=u(kt);e.load(io),e.load(cc),this._signalDisableRipple=!!this._parent&&pn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!ni(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Yg(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Lt(AF,7),i&2){let o;oe(o=se())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&$("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Zn("id",r.id),pe("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),ae("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",W]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:NF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Ke(RF),Ce(0,kF,1,2,"mat-pseudo-checkbox",1),le(1),m(2,"span",2,0),le(4,1),p(),Ce(5,OF,1,1,"mat-pseudo-checkbox",3),Ce(6,FF,2,1,"span",4),ue(7,"div",5)),i&2&&(Ee(r.multiple?0:-1),v(5),Ee(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),v(),Ee(r.group&&r.group._inert?6:-1),v(),x("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[aD,rw],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function cD(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function lD(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var dD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Se]})}return t})();var Qg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[Cs,dD,ar,Se]})}return t})();var PF=["trigger"],LF=["panel"],VF=[[["mat-select-trigger"]],"*"],jF=["mat-select-trigger","*"];function BF(t,n){if(t&1&&(m(0,"span",4),y(1),p()),t&2){let e=S();v(),it(e.placeholder)}}function UF(t,n){t&1&&le(0)}function HF(t,n){if(t&1&&(m(0,"span",11),y(1),p()),t&2){let e=S(2);v(),it(e.triggerValue)}}function zF(t,n){if(t&1&&(m(0,"span",5),Ce(1,UF,1,0)(2,HF,2,1,"span",11),p()),t&2){let e=S();v(),Ee(e.customTrigger?1:2)}}function $F(t,n){if(t&1){let e=at();m(0,"div",12,1),$("keydown",function(r){xe(e);let o=S();return Ie(o._handleKeydown(r))}),le(2,1),p()}if(t&2){let e=S();On(e.panelClass),ae("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),pe("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var GF=new _("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ce);return()=>xc(t)}}),WF=new _("MAT_SELECT_CONFIG"),qF=new _("MatSelectTrigger"),Xg=class{source;value;constructor(n,e){this.source=n,this.value=e}},Ju=(()=>{class t{_viewportRuler=u(uo);_changeDetectorRef=u(me);_elementRef=u(H);_dir=u(ti,{optional:!0});_idGenerator=u(wt);_renderer=u($e);_parentFormField=u(gc,{optional:!0});ngControl=u(Jn,{self:!0,optional:!0});_liveAnnouncer=u(Dg);_defaultOptions=u(WF,{optional:!0});_animationsDisabled=Ht();_popoverLocation;_initialized=new I;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=cD(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=lD(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Xg(this,e)}_scrollStrategyFactory=u(GF);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new I;_errorStateTracker;stateChanges=new I;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=ie(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Qr.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=br(()=>{let e=this.options;return e?e.changes.pipe(Zt(e),tt(()=>jn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(tt(()=>this.optionSelectionChanges))});openedChange=new X;_openedStream=this.openedChange.pipe(Me(e=>e),Q(()=>{}));_closedStream=this.openedChange.pipe(Me(e=>!e),Q(()=>{}));selectionChange=new X;valueChange=new X;constructor(){let e=u(Es),i=u(on,{optional:!0}),r=u(nc,{optional:!0}),o=u(new Zi("tabindex"),{optional:!0}),s=u(Ic,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new ws(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new bc(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ne(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ne(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Zt(null),Ne(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Yt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!ni(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!ni(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!ni(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch(o){return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof xs?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new lc(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=jn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ne(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),jn(...this.options.map(i=>i._stateChanges)).pipe(Ne(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=mt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Kn(o,qF,5)(o,ar,5)(o,Kg,5),i&2){let s;oe(s=se())&&(r.customTrigger=s.first),oe(s=se())&&(r.options=s),oe(s=se())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Lt(PF,5)(LF,5)(Xu,5),i&2){let o;oe(o=se())&&(r.trigger=o.first),oe(o=se())&&(r.panel=o.first),oe(o=se())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&$("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(pe("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),ae("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",W],disableRipple:[2,"disableRipple","disableRipple",W],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:yn(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",W],placeholder:"placeholder",required:[2,"required","required",W],multiple:[2,"multiple","multiple",W],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",W],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",yn],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",W]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Qe([{provide:mc,useExisting:t},{provide:Zg,useExisting:t}]),Ye],ngContentSelectors:jF,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Ke(VF),m(0,"div",2,0),$("click",function(){return r.open()}),m(3,"div",3),Ce(4,BF,2,1,"span",4)(5,zF,3,1,"span",5),p(),m(6,"div",6)(7,"div",7),zi(),m(8,"svg",8),ue(9,"path",9),p()()()(),re(10,$F,3,16,"ng-template",10),$("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=Yi(1);v(3),pe("id",r._valueId),v(),Ee(r.empty?4:5),v(6),x("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[xs,Xu],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var ef=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=z({imports:[qg,Qg,Se,zu,rt,Qg]})}return t})();function YF(t,n){t&1&&(m(0,"div",3),ue(1,"mat-spinner",4),p())}function ZF(t,n){if(t&1&&(m(0,"mat-option",17),y(1),p()),t&2){let e=n.$implicit;x("value",e),v(),Te(" ",e," ")}}function KF(t,n){if(t&1&&(m(0,"div",18),y(1),p()),t&2){let e=S(2);v(),Te(" ",e.mensagens," ")}}function QF(t,n){if(t&1){let e=at();m(0,"form",5),$("ngSubmit",function(){xe(e);let r=S();return Ie(r.onSubmit())})("keydown.enter",function(r){xe(e);let o=S();return r.preventDefault(),Ie(o.onSubmit())}),m(1,"mat-form-field",6)(2,"mat-label"),y(3,"Nome"),p(),m(4,"input",7),Oe("ngModelChange",function(r){xe(e);let o=S();return Ve(o.usuario.nome,r)||(o.usuario.nome=r),Ie(r)}),p(),Pe(),m(5,"mat-icon",8),y(6,"person"),p()(),m(7,"mat-form-field",6)(8,"mat-label"),y(9,"Email"),p(),m(10,"input",9),Oe("ngModelChange",function(r){xe(e);let o=S();return Ve(o.usuario.email,r)||(o.usuario.email=r),Ie(r)}),p(),Pe(),m(11,"mat-icon",8),y(12,"email"),p()(),m(13,"mat-form-field",6)(14,"mat-label"),y(15,"Nova senha"),p(),m(16,"input",10),Oe("ngModelChange",function(r){xe(e);let o=S();return Ve(o.usuario.senha,r)||(o.usuario.senha=r),Ie(r)}),p(),Pe(),m(17,"mat-icon",8),y(18,"lock"),p(),m(19,"mat-hint"),y(20," Deixe em branco para manter a senha atual. "),p()(),m(21,"mat-form-field",6)(22,"mat-label"),y(23,"Perfil"),p(),m(24,"mat-select",11),Oe("ngModelChange",function(r){xe(e);let o=S();return Ve(o.usuario.role,r)||(o.usuario.role=r),Ie(r)}),re(25,ZF,2,2,"mat-option",12),p(),Pe(),m(26,"mat-icon",8),y(27,"admin_panel_settings"),p()(),re(28,KF,2,1,"div",13),m(29,"div",14)(30,"button",15)(31,"mat-icon"),y(32,"save"),p(),y(33," Salvar "),p(),m(34,"button",16)(35,"mat-icon"),y(36,"arrow_back"),p(),y(37," Voltar "),p()()()}if(t&2){let e=S();v(4),ke("ngModel",e.usuario.nome),Le(),v(6),ke("ngModel",e.usuario.email),Le(),v(6),ke("ngModel",e.usuario.senha),Le(),v(8),ke("ngModel",e.usuario.role),Le(),v(),x("ngForOf",e.roles),v(3),x("ngIf",e.mensagens)}}var tf=class t{constructor(n,e,i,r,o){this.usuarioService=n;this.authService=e;this.route=i;this.router=r;this.cdr=o}usuarioService;authService;route;router;cdr;roles=["USER","ADMIN"];usuario={nome:"",email:"",senha:"",role:"USER"};carregando=!0;mensagens="";autoEdicao=!1;ngOnInit(){let n=this.route.snapshot.paramMap.get("id"),e=localStorage.getItem("id");console.log("idParam:",n),console.log("idLogado:",e);let i=localStorage.getItem("role")==="USER"||!n?e:n;console.log("id alvo:",i),i?this.usuarioService.porId(Number(i)).subscribe({next:r=>{this.usuario=j(C({},r),{senha:""}),this.autoEdicao=e!=null&&String(r.id)===e,this.carregando=!1,this.cdr.detectChanges()},error:r=>{console.error("Erro ao carregar usuario",r),this.mensagens="Usuario nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}):this.carregando=!1}onSubmit(){let n=C({},this.usuario);n.senha||delete n.senha,this.usuarioService.atualizar(n).subscribe({next:()=>{if(this.autoEdicao){this.authService.logout(),this.router.navigate(["/login"]);return}this.router.navigate(["/usuarios"])},error:e=>{console.error("Erro ao salvar usuario",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o usuario.",this.cdr.detectChanges()}}),this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(D(Ds),D(xn),D(rn),D(Xe),D(me))};static \u0275cmp=O({type:t,selectors:[["app-usuario-form"]],decls:6,vars:2,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","email","id","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","id","senha","name","senha",3,"ngModelChange","ngModel"],["id","role","name","role",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","success",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/usuarios","type","button"],[3,"value"],[1,"success"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card")(2,"h2"),y(3,"\u{1F464} Editar Usu\xE1rio"),p(),re(4,YF,2,0,"div",1)(5,QF,38,6,"form",2),p()()),e&2&&(v(4),x("ngIf",i.carregando),v(),x("ngIf",!i.carregando))},dependencies:[ct,bn,pt,jt,or,Et,Vt,rr,ei,Nt,on,Ct,Ut,Bt,rt,xt,gt,pc,an,$t,zt,ef,Ju,ar,Dt,Dn,Wt,Gt,ri,ii],encapsulation:2})};var Is=class t{constructor(n){this.http=n}http;listar(){return this.http.get(`${je.apiUrl}/pet/all`)}porId(n){return this.http.get(`${je.apiUrl}/pet/${n}`)}criar(n){return this.http.post(`${je.apiUrl}/pet`,n)}atualizar(n){return this.http.put(`${je.apiUrl}/pet`,n)}remover(n){return this.http.delete(`${je.apiUrl}/pet/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Cn))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};var XF=t=>["/pets",t];function JF(t,n){if(t&1){let e=at();m(0,"button",11),$("click",function(){xe(e);let r=S();return r.termoBusca="",Ie(r.buscarPets())}),m(1,"mat-icon"),y(2,"close"),p()()}}function e1(t,n){if(t&1&&(m(0,"div",12),y(1),p()),t&2){let e=S();v(),Te(" ",e.mensagens," ")}}function t1(t,n){t&1&&(m(0,"div",13),ue(1,"mat-spinner",14),p())}function n1(t,n){if(t&1){let e=at();m(0,"tr")(1,"td"),y(2),p(),m(3,"td"),y(4),p(),m(5,"td"),y(6),p(),m(7,"td",18)(8,"button",19)(9,"mat-icon"),y(10,"edit"),p()(),m(11,"button",20),$("click",function(){let r=xe(e).$implicit,o=S(2);return Ie(o.remover(r.id))}),m(12,"mat-icon"),y(13,"delete"),p()()()()}if(t&2){let e=n.$implicit;v(2),it(e.nome),v(2),it(e.especie),v(2),it(e.dono?.email??"-"),v(2),x("routerLink",ba(4,XF,e.id))}}function i1(t,n){if(t&1&&(m(0,"table",15)(1,"thead")(2,"tr")(3,"th"),y(4,"Nome"),p(),m(5,"th"),y(6,"Esp\xE9cie"),p(),m(7,"th"),y(8,"Dono"),p(),m(9,"th",16),y(10,"A\xE7\xF5es"),p()()(),m(11,"tbody"),re(12,n1,14,6,"tr",17),p()()),t&2){let e=S();v(12),x("ngForOf",e.petsBuscados)}}function r1(t,n){t&1&&(m(0,"p"),y(1,"Clique em "),m(2,"strong"),y(3,"Novo Pet"),p(),y(4," para come\xE7ar."),p())}function o1(t,n){t&1&&(m(0,"p"),y(1,"Tente outro termo de busca."),p())}function s1(t,n){if(t&1&&(m(0,"div",21)(1,"mat-icon",22),y(2,"pets"),p(),m(3,"h3"),y(4),p(),re(5,r1,5,0,"p",23)(6,o1,2,0,"p",23),p()),t&2){let e=S();v(4),it(e.termoBusca?"Nenhum pet encontrado":"Nenhum pet cadastrado"),v(),x("ngIf",!e.termoBusca),v(),x("ngIf",e.termoBusca)}}var nf=class t{constructor(n,e){this.petService=n;this.cdr=e}petService;cdr;pets=[];carregando=!0;mensagens="";termoBusca="";petsBuscados=[];ngOnInit(){this.carregar()}buscarPets(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.petsBuscados=this.pets;return}this.petsBuscados=this.pets.filter(e=>e.nome?.toLowerCase().includes(n)||e.especie?.toLowerCase().includes(n)||e.dono?.email?.toLowerCase().includes(n))}carregar(){this.carregando=!0,this.petService.listar().subscribe({next:n=>{this.pets=n,this.buscarPets(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Nao foi possivel carregar os pets.",this.carregando=!1,this.cdr.detectChanges()}})}remover(n){n!=null&&confirm("Confirma\xE7ao para remover")&&this.petService.remover(n).subscribe({next:()=>{this.pets=this.pets.filter(e=>e.id!==n),this.buscarPets(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover pet",e),this.mensagens="Nao foi possivel remover este pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(Is),D(me))};static \u0275cmp=O({type:t,selectors:[["app-gestao-pets"]],decls:20,vars:6,consts:[[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/pets/novo"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, esp\xE9cie ou dono...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),y(4,"\u{1F43E} Gest\xE3o de Pets"),p(),m(5,"button",2)(6,"mat-icon"),y(7,"add"),p(),y(8," Novo Pet "),p()(),m(9,"mat-form-field",3)(10,"mat-label"),y(11,"Pesquisar"),p(),m(12,"input",4),Oe("ngModelChange",function(o){return Ve(i.termoBusca,o)||(i.termoBusca=o),o}),$("ngModelChange",function(){return i.buscarPets()}),p(),Pe(),m(13,"mat-icon",5),y(14,"search"),p(),re(15,JF,3,0,"button",6),p(),re(16,e1,2,1,"div",7)(17,t1,2,0,"div",8)(18,i1,13,1,"table",9)(19,s1,7,3,"div",10),p()()),e&2&&(v(12),ke("ngModel",i.termoBusca),Le(),v(3),x("ngIf",i.termoBusca),v(),x("ngIf",i.mensagens),v(),x("ngIf",i.carregando),v(),x("ngIf",!i.carregando&&i.petsBuscados.length>0),v(),x("ngIf",!i.carregando&&i.petsBuscados.length===0))},dependencies:[ct,bn,pt,Ct,Ut,Bt,Dt,Dn,ro,Wt,Gt,ri,ii,rt,xt,gt,oo,an,$t,zt,jt,Et,Vt,Nt],encapsulation:2})};function a1(t,n){t&1&&(m(0,"div",3),ue(1,"mat-spinner",4),p())}function c1(t,n){if(t&1&&(m(0,"mat-option",15),y(1),p()),t&2){let e=n.$implicit;x("value",e),v(),Te(" ",e," ")}}function l1(t,n){if(t&1&&(m(0,"div",16),y(1),p()),t&2){let e=S(2);v(),Te(" ",e.mensagens," ")}}function d1(t,n){if(t&1){let e=at();m(0,"form",5),$("ngSubmit",function(){xe(e);let r=S();return Ie(r.onSubmit())})("keydown.enter",function(r){xe(e);let o=S();return r.preventDefault(),Ie(o.onSubmit())}),m(1,"mat-form-field",6)(2,"mat-label"),y(3,"Nome"),p(),m(4,"input",7),Oe("ngModelChange",function(r){xe(e);let o=S();return Ve(o.pet.nome,r)||(o.pet.nome=r),Ie(r)}),p(),Pe(),m(5,"mat-icon",8),y(6,"pets"),p()(),m(7,"mat-form-field",6)(8,"mat-label"),y(9,"Esp\xE9cie"),p(),m(10,"mat-select",9),Oe("ngModelChange",function(r){xe(e);let o=S();return Ve(o.pet.especie,r)||(o.pet.especie=r),Ie(r)}),re(11,c1,2,2,"mat-option",10),p(),Pe(),m(12,"mat-icon",8),y(13,"category"),p()(),re(14,l1,2,1,"div",11),m(15,"div",12)(16,"button",13)(17,"mat-icon"),y(18,"save"),p(),y(19," Salvar "),p(),m(20,"button",14)(21,"mat-icon"),y(22,"arrow_back"),p(),y(23," Voltar "),p()()()}if(t&2){let e=S();v(4),ke("ngModel",e.pet.nome),Le(),v(6),ke("ngModel",e.pet.especie),Le(),v(),x("ngForOf",e.especies),v(3),x("ngIf",e.mensagens)}}var Sc=class t{constructor(n,e,i,r){this.petService=n;this.route=e;this.router=i;this.cdr=r}petService;route;router;cdr;especies=["CACHORRO","GATO","PEIXE","ROEDOR","AVE","OUTRA"];pet={nome:"",especie:""};modoEdicao=!1;carregando=!1;mensagens="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&(this.modoEdicao=!0,this.carregando=!0,this.petService.porId(Number(n)).subscribe({next:e=>{this.pet=e,this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar pet",e),this.mensagens="Pet nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}))}onSubmit(){(this.modoEdicao?this.petService.atualizar(this.pet):this.petService.criar(this.pet)).subscribe({next:()=>{this.router.navigate(["/pets"])},error:e=>{console.error("Erro ao salvar pet",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(Is),D(rn),D(Xe),D(me))};static \u0275cmp=O({type:t,selectors:[["app-pet-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["id","especie","name","especie","required","",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","error",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/pets","type","button"],[3,"value"],[1,"error"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card")(2,"h2"),y(3),p(),re(4,a1,2,0,"div",1)(5,d1,24,4,"form",2),p()()),e&2&&(v(3),Te(" ",i.modoEdicao?"\u270F\uFE0F Editar Pet":"\u{1F43E} Novo Pet"," "),v(),x("ngIf",i.carregando),v(),x("ngIf",!i.carregando))},dependencies:[ct,bn,pt,jt,or,Et,Vt,rr,ei,Nt,on,Ct,Ut,Bt,rt,xt,gt,an,$t,zt,ef,Ju,ar,Dt,Dn,Wt,Gt,ri,ii],styles:[".actions[_ngcontent-%COMP%]{display:flex;gap:12px;margin-top:20px}.actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-width:140px}"]})};var ho=()=>{let t=u(Xe);return localStorage.getItem("token")?!0:(t.navigate(["/login"]),!1)};var Jg=()=>(localStorage.clear(),!0);var rf=class t{constructor(n){this.http=n}http;todas(){return this.http.get(`${je.apiUrl}/empresa/all`)}porId(n){return this.http.get(`${je.apiUrl}/empresa/${n}`)}porNome(n){return this.http.get(`${je.apiUrl}/empresa`,{params:{nome:n}})}atualizar(n){return this.http.put(`${je.apiUrl}/empresa`,n)}remover(n){return this.http.delete(`${je.apiUrl}/empresa/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Cn))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};function f1(t,n){if(t&1){let e=at();m(0,"button",10),$("click",function(){xe(e);let r=S();return r.termoBusca="",Ie(r.filtrarEmpresas())}),m(1,"mat-icon"),y(2,"close"),p()()}}function h1(t,n){if(t&1&&(m(0,"div",11),y(1),p()),t&2){let e=S();v(),Te(" ",e.mensagens," ")}}function p1(t,n){t&1&&(m(0,"div",12),ue(1,"mat-spinner",13),p())}function m1(t,n){if(t&1){let e=at();m(0,"tr")(1,"td"),y(2),p(),m(3,"td",17)(4,"button",18),$("click",function(){let r=xe(e).$implicit,o=S(2);return Ie(o.remover(r.id))}),m(5,"mat-icon"),y(6,"delete"),p()()()()}if(t&2){let e=n.$implicit;v(2),it(e.nome)}}function g1(t,n){if(t&1&&(m(0,"table",14)(1,"thead")(2,"tr")(3,"th"),y(4,"Nome"),p(),m(5,"th",15),y(6,"A\xE7\xF5es"),p()()(),m(7,"tbody"),re(8,m1,7,1,"tr",16),p()()),t&2){let e=S();v(8),x("ngForOf",e.empresasFiltradas)}}function v1(t,n){t&1&&(m(0,"p"),y(1,"N\xE3o h\xE1 empresas cadastradas."),p())}function y1(t,n){t&1&&(m(0,"p"),y(1,"Tente outro termo de busca."),p())}function _1(t,n){if(t&1&&(m(0,"div",19)(1,"mat-icon",20),y(2," group "),p(),m(3,"h3"),y(4),p(),re(5,v1,2,0,"p",21)(6,y1,2,0,"p",21),p()),t&2){let e=S();v(4),it((e.termoBusca,"Nenhuma empresa encontrada")),v(),x("ngIf",!e.termoBusca),v(),x("ngIf",e.termoBusca)}}var of=class t{constructor(n,e){this.empresaService=n;this.cdr=e}empresaService;cdr;empresas=[];carregando=!0;mensagens="";empresasFiltradas=[];termoBusca="";ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.empresaService.todas().subscribe({next:n=>{console.log(n),this.empresas=n,this.filtrarEmpresas(),console.log(this.empresasFiltradas),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar empresas.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarEmpresas(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.empresasFiltradas=this.empresas;return}this.empresasFiltradas=this.empresas.filter(e=>e.nome?.toLowerCase().includes(n))}remover(n){n!=null&&confirm("Confirma\xE7ao para remover esta empresa")&&this.empresaService.remover(n).subscribe({next:()=>{this.empresas=this.empresas.filter(e=>e.id!==n),this.filtrarEmpresas(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover empresa",e),this.mensagens="Nao foi possivel remover esta empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(D(rf),D(me))};static \u0275cmp=O({type:t,selectors:[["app-gestao-empresas"]],decls:16,vars:6,consts:[[1,"page"],[1,"header"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome da empresa...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),y(4,"\u{1F465} Gest\xE3o de Empresas"),p()(),m(5,"mat-form-field",2)(6,"mat-label"),y(7,"Pesquisar"),p(),m(8,"input",3),Oe("ngModelChange",function(o){return Ve(i.termoBusca,o)||(i.termoBusca=o),o}),$("ngModelChange",function(){return i.filtrarEmpresas()}),p(),Pe(),m(9,"mat-icon",4),y(10,"search"),p(),re(11,f1,3,0,"button",5),p(),re(12,h1,2,1,"div",6)(13,p1,2,0,"div",7)(14,g1,9,1,"table",8)(15,_1,7,3,"div",9),p()()),e&2&&(v(8),ke("ngModel",i.termoBusca),Le(),v(3),x("ngIf",i.termoBusca),v(),x("ngIf",i.mensagens),v(),x("ngIf",i.carregando),v(),x("ngIf",!i.carregando&&i.empresasFiltradas.length>0),v(),x("ngIf",!i.carregando&&i.empresasFiltradas.length===0))},dependencies:[jt,Et,Vt,Nt,ct,bn,pt,Ut,Bt,Dt,ro,Wt,Gt,ri,ii,rt,xt,gt,oo,an,$t,zt,Uu],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};var hD=[{path:"",redirectTo:"login",pathMatch:"full"},{path:"login",component:ju,canActivate:[Jg]},{path:"criar-usuario",component:Bu,canActivate:[Jg]},{path:"pets",component:nf,canActivate:[ho]},{path:"pets/novo",component:Sc,canActivate:[ho]},{path:"pets/:id",component:Sc,canActivate:[ho]},{path:"usuarios",component:Hu,canActivate:[ho]},{path:"usuarios/:id",component:tf,canActivate:[ho]},{path:"empresas",component:of,canActivate:[ho]}];var pD=(t,n)=>{let e=localStorage.getItem("token");return e&&(t=t.clone({setHeaders:{Authorization:`Bearer ${e}`}})),n(t)};var mD={providers:[Th(),cg(hD),Fm(Pm([pD]))]};function b1(t,n){if(t&1&&(m(0,"span",11)(1,"mat-icon"),y(2,"account_circle"),p(),y(3),p()),t&2){let e=S(2);v(3),Te(" ",e.emailUsuario," ")}}function C1(t,n){if(t&1){let e=at();m(0,"nav",1)(1,"div",2),ue(2,"span",3),p(),m(3,"div",4)(4,"a",5),y(5,"Pets"),p(),m(6,"a",6),y(7,"Usu\xE1rios"),p(),m(8,"a",7),y(9,"Empresas"),p()(),m(10,"div",8),re(11,b1,4,1,"span",9),m(12,"button",10),$("click",function(){xe(e);let r=S();return Ie(r.sair())}),m(13,"mat-icon"),y(14,"logout"),p(),y(15," Sair "),p()()()}if(t&2){let e=S();v(11),x("ngIf",e.logado)}}var sf=class t{constructor(n,e){this.authService=n;this.router=e}authService;router;get logado(){return this.authService.isLoggedIn()}get emailUsuario(){return localStorage.getItem("email")}sair(){this.authService.logout(),this.router.navigate(["/login"])}get isAdmin(){return localStorage.getItem("role")==="ADMIN"}static \u0275fac=function(e){return new(e||t)(D(xn),D(Xe))};static \u0275cmp=O({type:t,selectors:[["app-root"]],decls:2,vars:1,consts:[["class","top-nav",4,"ngIf"],[1,"top-nav"],[1,"nav-left"],[1,"logo"],[1,"nav-center"],["routerLink","/pets","routerLinkActive","active"],["routerLink","/usuarios","routerLinkActive","active"],["routerLink","/empresas","routerLinkActive","active"],[1,"nav-right"],["class","nav-user",4,"ngIf"],["mat-raised-button","","color","primary",3,"click"],[1,"nav-user"]],template:function(e,i){e&1&&(re(0,C1,16,1,"nav",0),ue(1,"router-outlet")),e&2&&x("ngIf",i.logado)},dependencies:[Ya,ct,pt,Ct,Wt,Gt,Dt,Dn],encapsulation:2})};Im(sf,mD).catch(t=>console.error(t));
